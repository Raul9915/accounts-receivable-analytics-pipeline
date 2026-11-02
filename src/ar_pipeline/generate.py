"""Generate synthetic, SAP-style accounts receivable "open items" exports.

The goal is realism without exposing any real company data:

* Customers have a segment, a payment-delay profile and a trend (improving, stable or
  deteriorating), so month-over-month analysis has real signal to find.
* Invoices are simulated over a backlog period and then "photographed" at each month end,
  exactly like an SAP open-items report (FBL5N-style) is extracted.
* A small, controlled share of data-quality issues is injected on purpose (duplicates,
  missing customers, invalid dates, zero amounts, inconsistent days in arrears) so the
  Silver layer has something real to detect.

Every number produced here is synthetic.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from ar_pipeline.config import Config, load_config

log = logging.getLogger(__name__)

SEGMENTS = np.array(["Corporate", "SME", "Retail", "Government"])
SEGMENT_PROBS = [0.15, 0.45, 0.30, 0.10]
SEGMENT_DELAY_DAYS = {"Corporate": 4.0, "SME": 14.0, "Retail": 18.0, "Government": 35.0}
SEGMENT_TERMS_DAYS = {"Corporate": 60, "SME": 30, "Retail": 30, "Government": 90}
SEGMENT_MONTHLY_INVOICES = {"Corporate": 3.0, "SME": 1.5, "Retail": 0.6, "Government": 1.2}
SEGMENT_AMOUNT_MULTIPLIER = {"Corporate": 2.0, "SME": 1.0, "Retail": 0.7, "Government": 1.5}

TRENDS = np.array(["improving", "stable", "deteriorating"])
TREND_PROBS = [0.20, 0.60, 0.20]
TREND_DRIFT_DAYS_PER_MONTH = {"improving": -2.5, "stable": 0.0, "deteriorating": 4.0}

BUSINESS_AREA_MIX = {
    "1000": {"VEH": 0.25, "PAR": 0.40, "SRV": 0.35},
    "2000": {"VEH": 0.35, "PAR": 0.35, "SRV": 0.30},
    "3000": {"PAR": 0.85, "SRV": 0.15},
}
# (median amount, lognormal sigma) per business area, in local currency
AMOUNT_PROFILE = {"VEH": (90_000.0, 0.5), "PAR": (3_000.0, 0.9), "SRV": (1_500.0, 0.8)}

NAME_PREFIXES = [
    "Andes", "Pacifico", "Inca", "Condor", "Sol", "Norte", "Sur", "Rimac", "Costa", "Sierra",
    "Selva", "Titicaca", "Misti", "Huascar", "Lomas", "Valle", "Puerto", "Cumbre", "Llama", "Vicuna",
]
NAME_CORES = [
    "Transportes", "Logistica", "Construcciones", "Agroindustrias", "Mineria", "Distribuidora",
    "Servicios", "Comercial", "Inversiones", "Ingenieria", "Cargo", "Turismo",
]
NAME_SUFFIXES = ["S.A.", "S.A.C.", "E.I.R.L.", "S.R.L."]

RAW_COLUMNS = [
    "Company Code", "Customer", "Customer Name", "Segment", "Document Number", "Document Type",
    "Business Area", "Posting Date", "Net Due Date", "Amount in LC", "Currency",
    "Days in Arrears", "Collector",
]


def build_customers(n_customers: int, rng: np.random.Generator) -> pd.DataFrame:
    """Create the customer master with segment, payment profile, trend and companies served."""
    segment = rng.choice(SEGMENTS, size=n_customers, p=SEGMENT_PROBS)
    trend = rng.choice(TRENDS, size=n_customers, p=TREND_PROBS)
    base_delay = np.array([SEGMENT_DELAY_DAYS[s] for s in segment]) * rng.lognormal(0.0, 0.6, n_customers) - 6

    names = (
        rng.choice(NAME_PREFIXES, n_customers).astype(object)
        + " "
        + rng.choice(NAME_CORES, n_customers).astype(object)
        + " "
        + rng.choice(NAME_SUFFIXES, n_customers).astype(object)
    )
    # Make names unique and readable, e.g. "Andes Transportes S.A. (0042)"
    ids = np.arange(1, n_customers + 1)
    names = [f"{name} {i:04d}" for name, i in zip(names, ids)]

    customers = pd.DataFrame(
        {
            "customer_id": [f"C{i:06d}" for i in ids],
            "customer_name": names,
            "segment": segment,
            "trend": trend,
            "base_delay": base_delay,
            "collector": rng.choice([f"COL-{i:02d}" for i in range(1, 7)], n_customers),
            "primary_company": rng.choice(["1000", "2000", "3000"], n_customers, p=[0.45, 0.35, 0.20]),
            "chronic_risk": rng.random(n_customers) < 0.04,  # small group that stops paying some invoices
        }
    )
    second = rng.choice(["1000", "2000", "3000"], n_customers)
    has_second = (rng.random(n_customers) < 0.30) & (second != customers["primary_company"].to_numpy())
    customers["second_company"] = np.where(has_second, second, None)
    return customers


def simulate_invoices(customers: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, rng: np.random.Generator) -> pd.DataFrame:
    """Simulate invoices and credit notes, including when (or whether) each one gets paid."""
    months = pd.date_range(start, end, freq="MS")

    relations = pd.concat(
        [
            customers.assign(company_code=customers["primary_company"]),
            customers.dropna(subset=["second_company"]).assign(company_code=lambda d: d["second_company"]),
        ],
        ignore_index=True,
    )

    frames = []
    for month_index, month_start in enumerate(months):
        lam = relations["segment"].map(SEGMENT_MONTHLY_INVOICES).to_numpy()
        counts = rng.poisson(lam)
        rows = relations.loc[relations.index.repeat(counts)].reset_index(drop=True)
        if rows.empty:
            continue
        days_in_month = month_start.days_in_month
        rows["posting_date"] = month_start + pd.to_timedelta(rng.integers(0, days_in_month, len(rows)), unit="D")
        rows["month_index"] = month_index
        frames.append(rows)

    docs = pd.concat(frames, ignore_index=True)
    n = len(docs)

    # Business area depends on the company's product mix
    business_area = np.empty(n, dtype=object)
    for company, mix in BUSINESS_AREA_MIX.items():
        mask = (docs["company_code"] == company).to_numpy()
        business_area[mask] = rng.choice(list(mix.keys()), mask.sum(), p=list(mix.values()))
    docs["business_area"] = business_area

    median = docs["business_area"].map(lambda ba: AMOUNT_PROFILE[ba][0]).to_numpy()
    sigma = docs["business_area"].map(lambda ba: AMOUNT_PROFILE[ba][1]).to_numpy()
    multiplier = docs["segment"].map(SEGMENT_AMOUNT_MULTIPLIER).to_numpy()
    amount = np.round(median * multiplier * rng.lognormal(0.0, sigma), 2)

    is_credit_note = rng.random(n) < 0.03
    docs["document_type"] = np.where(is_credit_note, "DG", "RV")
    docs["amount"] = np.where(is_credit_note, -np.round(amount * rng.uniform(0.05, 0.3, n), 2), amount)

    terms = docs["segment"].map(SEGMENT_TERMS_DAYS).to_numpy()
    terms = np.where(is_credit_note, 0, terms)
    docs["due_date"] = docs["posting_date"] + pd.to_timedelta(terms, unit="D")

    drift = docs["trend"].map(TREND_DRIFT_DAYS_PER_MONTH).to_numpy() * docs["month_index"].to_numpy()
    delay = docs["base_delay"].to_numpy() + drift + rng.normal(0, 10, n)
    delay = np.maximum(delay, -terms)  # nobody pays before the invoice exists
    delay = np.where(is_credit_note, rng.integers(5, 40, n), delay)
    docs["paid_date"] = docs["due_date"] + pd.to_timedelta(np.round(delay), unit="D")

    # A small share of invoices from chronically risky customers is never paid
    never_paid = docs["chronic_risk"].to_numpy() & (rng.random(n) < 0.35) & ~is_credit_note
    docs.loc[never_paid, "paid_date"] = pd.NaT

    # Document numbers: sequential per company, SAP-like
    docs = docs.sort_values(["company_code", "posting_date"]).reset_index(drop=True)
    seq = docs.groupby("company_code").cumcount() + 1
    docs["document_number"] = docs["company_code"].str[0] + "9" + seq.astype(str).str.zfill(8)
    return docs


def take_snapshot(docs: pd.DataFrame, snapshot: pd.Timestamp) -> pd.DataFrame:
    """Return the documents that are open (posted and not yet paid) at a month-end date."""
    is_open = (docs["posting_date"] <= snapshot) & (docs["paid_date"].isna() | (docs["paid_date"] > snapshot))
    snap = docs.loc[is_open].copy()
    snap["days_in_arrears"] = (snapshot - snap["due_date"]).dt.days
    return snap


def inject_quality_issues(raw: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Add a controlled share of realistic data-quality problems to a raw export."""
    raw = raw.copy()
    n = len(raw)

    def pick(share: float) -> np.ndarray:
        return rng.random(n) < share

    raw.loc[pick(0.0015), "Customer"] = ""
    bad_due = pick(0.001)
    raw.loc[bad_due, "Net Due Date"] = (
        pd.to_datetime(raw.loc[bad_due, "Posting Date"], format="%d.%m.%Y")
        - pd.to_timedelta(rng.integers(5, 40, bad_due.sum()), unit="D")
    ).dt.strftime("%d.%m.%Y")
    raw.loc[pick(0.001), "Amount in LC"] = "0.00"
    days_noise = pick(0.003)
    raw.loc[days_noise, "Days in Arrears"] = (
        raw.loc[days_noise, "Days in Arrears"].astype(int) + rng.choice([-3, -2, -1, 1, 2, 3], days_noise.sum())
    ).astype(str)

    messy_names = pick(0.06)
    raw.loc[messy_names, "Customer Name"] = (
        raw.loc[messy_names, "Customer Name"].str.lower().str.replace(" ", "  ", n=1, regex=False) + " "
    )

    duplicates = raw.loc[pick(0.004)]
    return pd.concat([raw, duplicates], ignore_index=True).sample(frac=1.0, random_state=int(rng.integers(1e9)))


def format_sap_export(snap: pd.DataFrame, currency: str) -> pd.DataFrame:
    """Format a snapshot like an SAP export: dd.mm.yyyy dates and trailing minus for credits."""
    amount = snap["amount"].map(lambda v: f"{abs(v):.2f}-" if v < 0 else f"{v:.2f}")
    return pd.DataFrame(
        {
            "Company Code": snap["company_code"],
            "Customer": snap["customer_id"],
            "Customer Name": snap["customer_name"],
            "Segment": snap["segment"],
            "Document Number": snap["document_number"],
            "Document Type": snap["document_type"],
            "Business Area": snap["business_area"],
            "Posting Date": snap["posting_date"].dt.strftime("%d.%m.%Y"),
            "Net Due Date": snap["due_date"].dt.strftime("%d.%m.%Y"),
            "Amount in LC": amount,
            "Currency": currency,
            "Days in Arrears": snap["days_in_arrears"].astype(str),
            "Collector": snap["collector"],
        }
    )[RAW_COLUMNS]


def generate(config: Config) -> list[Path]:
    """Generate all landing files and return their paths."""
    settings = config.synthetic
    rng = np.random.default_rng(settings["seed"])
    config.paths.landing.mkdir(parents=True, exist_ok=True)

    first_snapshot = pd.Timestamp(settings["first_snapshot"])
    snapshots = pd.date_range(first_snapshot, periods=settings["n_snapshots"], freq="ME")

    customers = build_customers(settings["n_customers"], rng)
    docs = simulate_invoices(customers, pd.Timestamp(settings["history_start"]), snapshots[-1], rng)
    log.info("Simulated %s documents for %s customers", f"{len(docs):,}", len(customers))

    written: list[Path] = []
    for snapshot in snapshots:
        snap = take_snapshot(docs, snapshot)
        for company_code, company_snap in snap.groupby("company_code"):
            raw = inject_quality_issues(format_sap_export(company_snap, settings["currency"]), rng)
            path = config.paths.landing / f"AR_OPEN_ITEMS_{company_code}_{snapshot:%Y%m%d}.csv"
            raw.to_csv(path, sep=";", index=False, encoding="utf-8")
            written.append(path)
    log.info("Wrote %s landing files to %s", len(written), config.paths.landing)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic SAP-style AR open-items exports.")
    parser.add_argument("--config", default=None, help="Path to pipeline.yaml")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    generate(load_config(args.config) if args.config else load_config())


if __name__ == "__main__":
    main()
