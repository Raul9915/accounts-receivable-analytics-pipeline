from __future__ import annotations

import duckdb

from ar_pipeline import bronze, generate, gold, quality, silver
from ar_pipeline.config import Config
from tests.conftest import write_landing


def _line(customer: str, doc: str, due: str, amount: str, days: int) -> str:
    return f"1000;{customer};NAME {customer};SME;{doc};RV;PAR;01.01.2025;{due};{amount};PEN;{days};COL-01"


def _build(config: Config) -> duckdb.DuckDBPyConnection:
    bronze.run(config)
    con = silver.run(config)
    gold.run(con, config)
    return con


def test_customer_trend_status(config: Config) -> None:
    write_landing(
        config,
        "AR_OPEN_ITEMS_1000_20251130.csv",
        [
            _line("C1", "1900000001", "15.11.2025", "1000.00", 15),   # 0-30
            _line("C2", "1900000002", "01.09.2025", "1000.00", 90),   # 61-90
            _line("C3", "1900000003", "15.11.2025", "1000.00", 15),   # 0-30
            _line("C5", "1900000005", "15.11.2025", "1000.00", 15),   # settles before next month
        ],
    )
    write_landing(
        config,
        "AR_OPEN_ITEMS_1000_20251231.csv",
        [
            _line("C1", "1900000001", "15.11.2025", "1000.00", 46),   # 31-60 -> worse bucket
            _line("C2", "1900000010", "15.12.2025", "500.00", 16),    # 0-30  -> better bucket
            _line("C3", "1900000003", "15.11.2025", "1000.00", 16),   # same bucket, same overdue
            _line("C4", "1900000004", "15.01.2026", "800.00", -15),   # first time seen
        ],
    )
    con = _build(config)

    status = dict(
        con.execute(
            "SELECT customer_id, trend_status FROM customer_monthly WHERE snapshot_date = DATE '2025-12-31'"
        ).fetchall()
    )
    assert status == {"C1": "Deteriorated", "C2": "Improved", "C3": "Stable", "C4": "New"}

    baseline = con.execute(
        "SELECT DISTINCT trend_status FROM customer_monthly WHERE snapshot_date = DATE '2025-11-30'"
    ).fetchall()
    assert baseline == [("Baseline",)]

    settled = con.execute(
        "SELECT customers FROM bucket_transitions WHERE from_bucket_id = 2 AND to_bucket_id = 0"
    ).fetchall()
    assert settled == [(1,)]  # C5 paid everything


def test_credit_notes_do_not_define_worst_bucket(config: Config) -> None:
    write_landing(
        config,
        "AR_OPEN_ITEMS_1000_20251231.csv",
        [
            _line("C1", "1900000001", "15.01.2026", "1000.00", -15),
            "1000;C1;NAME C1;SME;1900000002;DG;PAR;01.06.2025;01.06.2025;200.00-;PEN;213;COL-01",
        ],
    )
    con = _build(config)
    row = con.execute("SELECT total_balance_lc, worst_bucket_id FROM customer_monthly").fetchall()[0]
    assert float(row[0]) == 800.0
    assert row[1] == 1  # the old credit note must not make the customer look >90 days late


def test_full_pipeline_reconciles_on_synthetic_data(config: Config) -> None:
    generate.generate(config)
    con = _build(config)
    report = quality.run(con, config)  # raises ReconciliationError if anything is lost
    assert report.passed
    assert report.silver_rejected_rows > 0  # injected issues are caught
    assert report.snapshots == 3
