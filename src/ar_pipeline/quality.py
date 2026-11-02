"""Data quality checks and reconciliation between layers.

The pipeline fails loudly if rows or amounts are lost between layers; rejected rows are
expected and reported, missing rows are a bug.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field

import duckdb

from ar_pipeline.config import Config

log = logging.getLogger(__name__)


class ReconciliationError(RuntimeError):
    """Raised when rows or amounts do not reconcile between layers."""


@dataclass
class QualityReport:
    bronze_rows: int
    silver_accepted_rows: int
    silver_rejected_rows: int
    rejected_by_reason: dict[str, int]
    days_in_arrears_mismatches: int
    fact_rows: int
    fact_amount_lc: float
    silver_amount_lc: float
    snapshots: int
    checks: dict[str, bool] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(self.checks.values())


def evaluate(con: duckdb.DuckDBPyConnection) -> QualityReport:
    scalar = lambda sql: con.execute(sql).fetchall()[0][0]  # noqa: E731

    report = QualityReport(
        bronze_rows=scalar("SELECT count(*) FROM silver_validated"),
        silver_accepted_rows=scalar("SELECT count(*) FROM silver_open_items"),
        silver_rejected_rows=scalar("SELECT count(*) FROM silver_rejected"),
        rejected_by_reason=dict(
            con.execute(
                "SELECT rejection_reason, count(*) FROM silver_rejected GROUP BY 1 ORDER BY 2 DESC"
            ).fetchall()
        ),
        days_in_arrears_mismatches=scalar("SELECT count(*) FROM silver_open_items WHERE days_in_arrears_mismatch"),
        fact_rows=scalar("SELECT count(*) FROM fact_ar_open_items"),
        fact_amount_lc=float(scalar("SELECT sum(amount_lc) FROM fact_ar_open_items")),
        silver_amount_lc=float(scalar("SELECT sum(amount_lc) FROM silver_open_items")),
        snapshots=scalar("SELECT count(*) FROM dim_date"),
    )
    report.checks = {
        "bronze_rows_equal_accepted_plus_rejected": report.bronze_rows
        == report.silver_accepted_rows + report.silver_rejected_rows,
        "fact_rows_equal_silver_accepted": report.fact_rows == report.silver_accepted_rows,
        "fact_amount_equal_silver_amount": abs(report.fact_amount_lc - report.silver_amount_lc) < 0.01,
        "no_duplicate_documents_in_fact": scalar(
            """SELECT count(*) FROM (SELECT snapshot_date, company_code, document_number
               FROM fact_ar_open_items GROUP BY ALL HAVING count(*) > 1)"""
        )
        == 0,
        "every_fact_row_has_customer": scalar(
            "SELECT count(*) FROM fact_ar_open_items f ANTI JOIN dim_customer d USING (customer_id)"
        )
        == 0,
        "every_fact_row_has_aging_bucket": scalar("SELECT count(*) FROM fact_ar_open_items WHERE aging_bucket_id IS NULL")
        == 0,
        "customer_monthly_reconciles_to_fact": abs(
            float(scalar("SELECT sum(total_balance_lc) FROM customer_monthly")) - report.fact_amount_lc
        )
        < 0.01,
    }
    return report


def write(report: QualityReport, config: Config) -> None:
    config.paths.reports.mkdir(parents=True, exist_ok=True)
    (config.paths.reports / "data_quality_report.json").write_text(
        json.dumps({**asdict(report), "passed": report.passed}, indent=2), encoding="utf-8"
    )

    reject_share = report.silver_rejected_rows / report.bronze_rows if report.bronze_rows else 0
    lines = [
        "# Data Quality Report",
        "",
        "> Generated automatically by the pipeline. All data is synthetic.",
        "",
        "## Row flow",
        "",
        "| Layer | Rows |",
        "|---|---:|",
        f"| Bronze (raw rows ingested) | {report.bronze_rows:,} |",
        f"| Silver - accepted | {report.silver_accepted_rows:,} |",
        f"| Silver - rejected | {report.silver_rejected_rows:,} ({reject_share:.2%}) |",
        f"| Gold - fact_ar_open_items | {report.fact_rows:,} |",
        "",
        "## Rejected rows by rule",
        "",
        "| Rule | Rows |",
        "|---|---:|",
        *[f"| `{reason}` | {count:,} |" for reason, count in report.rejected_by_reason.items()],
        "",
        "## Control: SAP days in arrears vs. recalculated from dates",
        "",
        f"{report.days_in_arrears_mismatches:,} accepted rows have a different value when days in arrears is "
        "recalculated from the due date. SAP remains the official value; these rows are flagged "
        "(`days_in_arrears_mismatch`) for follow-up with the source system owner.",
        "",
        "## Reconciliation checks",
        "",
        "| Check | Result |",
        "|---|---|",
        *[f"| {name.replace('_', ' ')} | {'PASS' if ok else 'FAIL'} |" for name, ok in report.checks.items()],
        "",
    ]
    (config.paths.reports / "data_quality_report.md").write_text("\n".join(lines), encoding="utf-8")


def run(con: duckdb.DuckDBPyConnection, config: Config) -> QualityReport:
    report = evaluate(con)
    write(report, config)
    if not report.passed:
        failed = [name for name, ok in report.checks.items() if not ok]
        raise ReconciliationError(f"Reconciliation failed: {failed}")
    log.info("Quality: all %s reconciliation checks passed", len(report.checks))
    return report
