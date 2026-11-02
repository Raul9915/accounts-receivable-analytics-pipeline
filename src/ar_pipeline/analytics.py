"""Business KPIs and README charts computed from the Gold layer."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa: E402

from ar_pipeline.config import Config  # noqa: E402

log = logging.getLogger(__name__)

# Validated reference palette (light mode). See docs in the README for the rationale.
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a"]
# Ordinal ramp for aging buckets (one hue, light -> dark = older debt)
AGING_RAMP = ["#b7d3f6", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]
STATUS = {"Improved": "#0ca30c", "Stable": "#b8b7b1", "Deteriorated": "#d03b3b", "New": "#2a78d6"}


def compute_kpis(con: duckdb.DuckDBPyConnection) -> dict:
    """Headline KPIs for the first and latest snapshot plus portfolio dynamics."""
    by_month = con.execute(
        """
        SELECT
            snapshot_date,
            sum(amount_lc)                                         AS total,
            sum(amount_lc) FILTER (WHERE is_overdue)               AS overdue,
            sum(amount_lc) FILTER (WHERE aging_bucket_id = 5)      AS over_90,
            count(DISTINCT customer_id)                            AS customers,
            count(DISTINCT customer_id) FILTER (WHERE aging_bucket_id = 5 AND amount_lc > 0) AS customers_over_90
        FROM fact_ar_open_items
        GROUP BY snapshot_date ORDER BY snapshot_date
        """
    ).df()
    by_month["overdue_ratio"] = by_month["overdue"] / by_month["total"]
    by_month["over_90_ratio"] = by_month["over_90"] / by_month["total"]

    first, last = by_month.iloc[0], by_month.iloc[-1]
    latest = last["snapshot_date"]

    top10_share = con.execute(
        """
        WITH overdue AS (
            SELECT customer_id, overdue_balance_lc FROM customer_monthly
            WHERE snapshot_date = ? AND overdue_balance_lc > 0
        )
        SELECT sum(overdue_balance_lc) FILTER (WHERE rn <= 10) / sum(overdue_balance_lc)
        FROM (SELECT *, row_number() OVER (ORDER BY overdue_balance_lc DESC) AS rn FROM overdue)
        """,
        [latest],
    ).fetchall()[0][0]

    trend = dict(
        con.execute(
            "SELECT trend_status, count(*) FROM customer_monthly WHERE snapshot_date = ? GROUP BY 1", [latest]
        ).fetchall()
    )

    # Roll rate: share of customers whose worst bucket was 0-30 that rolled to 31-60 or worse a month later
    roll = con.execute(
        """
        SELECT
            sum(customers) FILTER (WHERE to_bucket_id >= 3) / sum(customers)  AS roll_forward,
            sum(customers) FILTER (WHERE to_bucket_id IN (0, 1)) / sum(customers) AS cured
        FROM bucket_transitions WHERE from_bucket_id = 2
        """
    ).fetchall()[0]

    persistent_90 = con.execute(
        """
        SELECT sum(customers) FILTER (WHERE to_bucket_id = 5) / sum(customers)
        FROM bucket_transitions WHERE from_bucket_id = 5
        """
    ).fetchall()[0][0]

    def snap(row: pd.Series) -> dict:
        return {
            "snapshot_date": str(pd.Timestamp(row["snapshot_date"]).date()),
            "total_balance_lc": round(float(row["total"]), 2),
            "overdue_balance_lc": round(float(row["overdue"]), 2),
            "overdue_ratio": round(float(row["overdue_ratio"]), 4),
            "over_90_balance_lc": round(float(row["over_90"]), 2),
            "over_90_ratio": round(float(row["over_90_ratio"]), 4),
            "customers": int(row["customers"]),
            "customers_over_90": int(row["customers_over_90"]),
        }

    return {
        "note": "All figures are computed from synthetic data.",
        "first_snapshot": snap(first),
        "latest_snapshot": snap(last),
        "latest_top10_overdue_concentration": round(float(top10_share), 4),
        "latest_customer_trend": {k: int(v) for k, v in trend.items()},
        "avg_roll_rate_0_30_to_31_plus": round(float(roll[0]), 4),
        "avg_cure_rate_0_30_to_current_or_settled": round(float(roll[1]), 4),
        "avg_persistence_over_90": round(float(persistent_90), 4),
    }


def _style(ax: plt.Axes, title: str, subtitle: str | None = None) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_title(title, loc="left", fontsize=13, color=TEXT_PRIMARY, fontweight="bold", pad=22)
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=9.5, color=TEXT_SECONDARY)


def _new_figure(width: float = 9.0, height: float = 4.6) -> tuple[plt.Figure, plt.Axes]:
    fig, ax = plt.subplots(figsize=(width, height), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    return fig, ax


def _save(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)


millions = FuncFormatter(lambda v, _: f"{v / 1e6:,.0f}M")


def chart_aging_evolution(con: duckdb.DuckDBPyConnection, labels: dict[int, str], path: Path) -> None:
    data = con.execute(
        """
        SELECT strftime(snapshot_date, '%b') AS month, snapshot_date, aging_bucket_id, sum(balance_lc) AS balance
        FROM agg_portfolio_monthly GROUP BY ALL ORDER BY snapshot_date, aging_bucket_id
        """
    ).df()
    pivot = data.pivot_table(index=["snapshot_date", "month"], columns="aging_bucket_id", values="balance").fillna(0)
    months = [m for _, m in pivot.index]
    fig, ax = _new_figure()
    bottom = np.zeros(len(pivot))
    for i, bucket in enumerate(sorted(pivot.columns)):
        values = pivot[bucket].clip(lower=0).to_numpy(dtype=float)
        ax.bar(months, values, bottom=bottom, width=0.62, color=AGING_RAMP[i], label=labels[bucket],
               edgecolor=SURFACE, linewidth=1.2)
        bottom += values
    ax.yaxis.set_major_formatter(millions)
    _style(ax, "Receivables portfolio by aging bucket", "Month-end open balance, PEN (synthetic data, 2025)")
    ax.legend(ncol=5, frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(0, -0.08), labelcolor=TEXT_SECONDARY)
    _save(fig, path)


def chart_overdue_ratio_by_company(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    data = con.execute(
        """
        SELECT f.snapshot_date, c.company_name,
               sum(f.amount_lc) FILTER (WHERE f.is_overdue) / sum(f.amount_lc) AS overdue_ratio
        FROM fact_ar_open_items f JOIN dim_company c USING (company_code)
        GROUP BY ALL ORDER BY ALL
        """
    ).df()
    data["month"] = pd.to_datetime(data["snapshot_date"]).dt.strftime("%b")
    fig, ax = _new_figure()
    end_labels = []
    for color, (company, group) in zip(CATEGORICAL, data.groupby("company_name", sort=True)):
        x = np.arange(len(group))
        ax.plot(x, group["overdue_ratio"], color=color, linewidth=2, marker="o", markersize=4.5, label=company)
        end_labels.append([float(group["overdue_ratio"].iloc[-1]), len(group) - 1])
    # Spread end-of-line labels so they never overlap
    end_labels.sort()
    for i in range(1, len(end_labels)):
        end_labels[i][0] = max(end_labels[i][0], end_labels[i - 1][0] + 0.022)
    for (y_label, x_last), value in zip(end_labels, sorted(float(v) for v in data.groupby("company_name")["overdue_ratio"].last())):
        ax.annotate(f"{value:.0%}", (x_last, y_label), xytext=(8, 0), textcoords="offset points", va="center",
                    fontsize=9, color=TEXT_PRIMARY, annotation_clip=False)
    months = data.drop_duplicates("snapshot_date")["month"].tolist()
    ax.set_xticks(range(len(months)), months)
    ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.set_ylim(0, data["overdue_ratio"].max() * 1.15)
    _style(ax, "Overdue share of portfolio by company", "Overdue balance / total open balance at month end")
    ax.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(0, -0.08), ncol=3, labelcolor=TEXT_SECONDARY)
    _save(fig, path)


def chart_roll_rates(con: duckdb.DuckDBPyConnection, labels: dict[int, str], path: Path) -> None:
    data = con.execute(
        "SELECT from_bucket_id, to_bucket_id, sum(customers) AS customers FROM bucket_transitions GROUP BY ALL"
    ).df()
    matrix = data.pivot_table(index="from_bucket_id", columns="to_bucket_id", values="customers", fill_value=0)
    matrix = matrix.div(matrix.sum(axis=1), axis=0)
    row_labels = [labels[i] for i in matrix.index]
    col_labels = ["Settled" if i == 0 else labels[i] for i in matrix.columns]

    cmap = matplotlib.colors.LinearSegmentedColormap.from_list("seq", ["#f4f8fd", "#86b6ef", "#256abf", "#0d366b"])
    fig, ax = _new_figure(8.0, 4.8)
    ax.imshow(matrix.to_numpy(), cmap=cmap, vmin=0, vmax=1, aspect="auto")
    for (r, c), value in np.ndenumerate(matrix.to_numpy()):
        ax.text(c, r, f"{value:.0%}", ha="center", va="center", fontsize=9.5,
                color="white" if value > 0.45 else TEXT_PRIMARY)
    ax.set_xticks(range(len(col_labels)), col_labels)
    ax.set_yticks(range(len(row_labels)), row_labels)
    ax.set_xlabel("Worst bucket next month", color=TEXT_SECONDARY, fontsize=9.5)
    ax.set_ylabel("Worst bucket this month", color=TEXT_SECONDARY, fontsize=9.5)
    _style(ax, "Delinquency roll-rate matrix", "Share of customers moving between worst aging buckets, average of 11 month pairs")
    ax.grid(False)
    ax.spines["bottom"].set_visible(False)
    _save(fig, path)


def chart_customer_trend(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    data = con.execute(
        """
        SELECT snapshot_date, trend_status, count(*) AS customers
        FROM customer_monthly WHERE trend_status <> 'Baseline'
        GROUP BY ALL ORDER BY ALL
        """
    ).df()
    pivot = data.pivot_table(index="snapshot_date", columns="trend_status", values="customers", fill_value=0)
    order = [s for s in ["Improved", "Stable", "Deteriorated", "New"] if s in pivot.columns]
    months = [pd.Timestamp(d).strftime("%b") for d in pivot.index]
    fig, ax = _new_figure()
    bottom = np.zeros(len(pivot))
    for status in order:
        values = pivot[status].to_numpy(dtype=float)
        ax.bar(months, values, bottom=bottom, width=0.62, color=STATUS[status], label=status, edgecolor=SURFACE,
               linewidth=1.2)
        bottom += values
    _style(ax, "Customer payment-behavior trend", "Customers with open items, classified vs. previous month")
    ax.legend(ncol=4, frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(0, -0.08), labelcolor=TEXT_SECONDARY)
    _save(fig, path)


def run(con: duckdb.DuckDBPyConnection, config: Config, render_charts: bool = True) -> dict:
    kpis = compute_kpis(con)
    config.paths.reports.mkdir(parents=True, exist_ok=True)
    (config.paths.reports / "kpi_summary.json").write_text(json.dumps(kpis, indent=2), encoding="utf-8")

    if render_charts:
        config.paths.images.mkdir(parents=True, exist_ok=True)
        labels = {b["id"]: b["label"] for b in config.aging_buckets}
        chart_aging_evolution(con, labels, config.paths.images / "aging_evolution.png")
        chart_overdue_ratio_by_company(con, config.paths.images / "overdue_ratio_by_company.png")
        chart_roll_rates(con, labels, config.paths.images / "roll_rate_matrix.png")
        chart_customer_trend(con, config.paths.images / "customer_trend.png")
        log.info("Analytics: KPIs and 4 charts written")
    return kpis
