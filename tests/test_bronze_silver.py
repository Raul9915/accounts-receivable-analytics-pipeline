from __future__ import annotations

import duckdb
import pytest

from ar_pipeline import bronze, silver
from ar_pipeline.config import Config
from tests.conftest import write_landing

VALID = "1000;C000001;  acme   transportes s.a. ;SME;1900000001;RV;PAR;01.12.2025;31.12.2025;1500.00;PEN;0;COL-01"


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def _run(config: Config, con: duckdb.DuckDBPyConnection) -> None:
    bronze.run(config)
    silver.build(con, str(config.paths.bronze / "ar_open_items" / "**" / "*.parquet"), config.aging_buckets)


@pytest.mark.parametrize(
    ("days", "expected_bucket"),
    [(-45, 1), (-1, 1), (0, 2), (30, 2), (31, 3), (60, 3), (61, 4), (90, 4), (91, 5), (400, 5)],
)
def test_aging_bucket_boundaries(config: Config, days: int, expected_bucket: int) -> None:
    case = silver.aging_bucket_case(config.aging_buckets, column="d")
    result = duckdb.sql(f"SELECT {case} FROM (SELECT {days} AS d)").fetchall()[0][0]
    assert result == expected_bucket


def test_silver_types_cleans_and_rejects(config: Config, con: duckdb.DuckDBPyConnection) -> None:
    write_landing(
        config,
        "AR_OPEN_ITEMS_1000_20251231.csv",
        [
            VALID,
            VALID,  # exact duplicate
            "1000;;NO CUSTOMER;SME;1900000002;RV;PAR;01.12.2025;31.12.2025;100.00;PEN;0;COL-01",
            "1000;C000003;BAD DATES;SME;1900000003;RV;PAR;20.12.2025;01.12.2025;100.00;PEN;30;COL-01",
            "1000;C000004;ZERO;SME;1900000004;RV;PAR;01.12.2025;31.12.2025;0.00;PEN;0;COL-01",
            "1000;C000005;CREDIT NOTE;SME;1900000005;DG;PAR;10.12.2025;10.12.2025;250.50-;PEN;21;COL-01",
            "1000;C000006;OLD DEBT;Retail;1900000006;RV;VEH;01.06.2025;01.07.2025;9000.00;PEN;183;COL-02",
        ],
    )
    _run(config, con)

    reasons = dict(con.execute("SELECT rejection_reason, count(*) FROM silver_rejected GROUP BY 1").fetchall())
    assert reasons == {
        "duplicate_document": 1,
        "missing_customer": 1,
        "due_date_before_posting_date": 1,
        "zero_amount": 1,
    }

    rows = {
        r[0]: r[1:]
        for r in con.execute(
            "SELECT document_number, customer_name, amount_lc, aging_bucket_id, is_overdue FROM silver_open_items"
        ).fetchall()
    }
    assert set(rows) == {"1900000001", "1900000005", "1900000006"}
    assert rows["1900000001"][0] == "ACME TRANSPORTES S.A."  # trimmed, upper-cased, single spaces
    assert float(rows["1900000005"][1]) == -250.50  # SAP trailing minus -> negative amount
    assert rows["1900000006"][2:] == (5, True)  # >90 days


def test_days_in_arrears_mismatch_is_flagged_not_rejected(config: Config, con: duckdb.DuckDBPyConnection) -> None:
    # Due 01.12.2025, snapshot 31.12.2025 -> 30 days from dates, but SAP says 33
    write_landing(
        config,
        "AR_OPEN_ITEMS_1000_20251231.csv",
        ["1000;C000001;ACME;SME;1900000001;RV;PAR;01.11.2025;01.12.2025;100.00;PEN;33;COL-01"],
    )
    _run(config, con)
    row = con.execute(
        "SELECT days_in_arrears, days_in_arrears_check, days_in_arrears_mismatch, aging_bucket_id FROM silver_open_items"
    ).fetchall()[0]
    assert row == (33, 30, True, 3)  # SAP value drives the bucket (31-60)


def test_company_code_must_match_file_name(config: Config, con: duckdb.DuckDBPyConnection) -> None:
    write_landing(config, "AR_OPEN_ITEMS_1000_20251231.csv", [VALID.replace("1000;", "2000;", 1)])
    _run(config, con)
    assert con.execute("SELECT rejection_reason FROM silver_rejected").fetchall() == [("company_code_mismatch",)]


def test_bronze_never_overwrites_existing_snapshot(config: Config) -> None:
    write_landing(config, "AR_OPEN_ITEMS_1000_20251231.csv", [VALID])
    assert [r.status for r in bronze.run(config)] == ["loaded"]
    assert [r.status for r in bronze.run(config)] == ["skipped_existing"]
    assert [r.status for r in bronze.run(config, force=True)] == ["loaded"]


def test_bronze_quarantines_unexpected_files(config: Config) -> None:
    write_landing(config, "random_export.csv", [VALID])
    (config.paths.landing / "AR_OPEN_ITEMS_1000_20251130.csv").write_text("Customer;Amount\nC1;10\n", encoding="utf-8")
    results = {r.file: r for r in bronze.run(config)}
    assert results["random_export.csv"].status == "quarantined"
    assert results["AR_OPEN_ITEMS_1000_20251130.csv"].status == "quarantined"
    assert "missing columns" in results["AR_OPEN_ITEMS_1000_20251130.csv"].detail
