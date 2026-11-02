"""Silver layer: typed, cleaned, deduplicated and validated open items.

Each Bronze row ends up in exactly one of two outputs:

* ``ar_open_items``     - rows that pass every rule, enriched with the aging bucket.
* ``rejected_records``  - rows that fail a rule, with the first rule they failed.

"Days in Arrears" exported by SAP is the official source for aging. The pipeline also
recalculates it from the dates, but only as a control: mismatches are flagged, never
silently "fixed".
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb

from ar_pipeline.config import Config

log = logging.getLogger(__name__)

# Ordered rules: a row is rejected with the first rule it breaks.
REJECTION_RULES: list[tuple[str, str]] = [
    ("duplicate_document", "dup_rank > 1"),
    ("company_code_mismatch", "source_company_code IS DISTINCT FROM company_code"),
    ("missing_customer", "customer_id IS NULL"),
    ("missing_document_number", "document_number IS NULL"),
    ("invalid_posting_date", "posting_date IS NULL"),
    ("invalid_due_date", "due_date IS NULL"),
    ("due_date_before_posting_date", "due_date < posting_date"),
    ("posting_date_after_snapshot", "posting_date > snapshot_date"),
    ("invalid_amount", "amount_lc IS NULL"),
    ("zero_amount", "amount_lc = 0"),
    ("invalid_days_in_arrears", "days_in_arrears IS NULL"),
]


def aging_bucket_case(buckets: list[dict[str, Any]], column: str = "days_in_arrears") -> str:
    """Build a SQL CASE expression that maps days in arrears to an aging bucket id."""
    whens = []
    for bucket in buckets:
        conditions = []
        if bucket["min_days"] is not None:
            conditions.append(f"{column} >= {int(bucket['min_days'])}")
        if bucket["max_days"] is not None:
            conditions.append(f"{column} <= {int(bucket['max_days'])}")
        whens.append(f"WHEN {' AND '.join(conditions)} THEN {int(bucket['id'])}")
    return "CASE " + " ".join(whens) + " END"


def validated_sql(bronze_glob: str, buckets: list[dict[str, Any]]) -> str:
    """SQL that types every Bronze row and assigns its rejection reason (NULL if accepted)."""
    rejection_case = "CASE " + " ".join(f"WHEN {rule} THEN '{name}'" for name, rule in REJECTION_RULES) + " END"
    return f"""
    WITH bronze AS (
        SELECT * FROM read_parquet('{bronze_glob}', hive_partitioning = true, hive_types_autocast = false)
    ),
    typed AS (
        SELECT
            CAST(snapshot_date AS DATE)                                         AS snapshot_date,
            CAST(company_code AS VARCHAR)                                       AS company_code,
            NULLIF(trim(source_company_code), '')                               AS source_company_code,
            NULLIF(upper(trim(customer_id)), '')                                AS customer_id,
            regexp_replace(upper(trim(customer_name)), '\\s+', ' ', 'g')        AS customer_name,
            trim(segment)                                                       AS segment,
            NULLIF(trim(document_number), '')                                   AS document_number,
            upper(trim(document_type))                                          AS document_type,
            upper(trim(business_area))                                          AS business_area,
            CAST(try_strptime(trim(posting_date), '%d.%m.%Y') AS DATE)          AS posting_date,
            CAST(try_strptime(trim(net_due_date), '%d.%m.%Y') AS DATE)          AS due_date,
            CASE
                WHEN trim(amount_lc) LIKE '%-'
                    THEN -TRY_CAST(rtrim(trim(amount_lc), '-') AS DECIMAL(18, 2))
                ELSE TRY_CAST(trim(amount_lc) AS DECIMAL(18, 2))
            END                                                                 AS amount_lc,
            upper(trim(currency))                                               AS currency,
            TRY_CAST(trim(days_in_arrears) AS INTEGER)                          AS days_in_arrears,
            trim(collector)                                                     AS collector,
            _source_file,
            _ingested_at
        FROM bronze
    ),
    ranked AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY snapshot_date, company_code, document_number
                ORDER BY _source_file, _ingested_at
            ) AS dup_rank
        FROM typed
    )
    SELECT
        *,
        {rejection_case} AS rejection_reason
    FROM ranked
    """


def build(con: duckdb.DuckDBPyConnection, bronze_glob: str, buckets: list[dict[str, Any]]) -> None:
    """Create the ``silver_validated``, ``silver_open_items`` and ``silver_rejected`` tables in ``con``."""
    con.execute(f"CREATE OR REPLACE TABLE silver_validated AS {validated_sql(bronze_glob, buckets)}")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver_open_items AS
        SELECT
            snapshot_date, company_code, customer_id, customer_name, segment, collector,
            document_number, document_type, business_area, posting_date, due_date,
            amount_lc, currency, days_in_arrears,
            {aging_bucket_case(buckets)}                                     AS aging_bucket_id,
            days_in_arrears >= 0                                             AS is_overdue,
            date_diff('day', due_date, snapshot_date)                        AS days_in_arrears_check,
            date_diff('day', due_date, snapshot_date) <> days_in_arrears     AS days_in_arrears_mismatch,
            _source_file
        FROM silver_validated
        WHERE rejection_reason IS NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_rejected AS
        SELECT * EXCLUDE (dup_rank) FROM silver_validated WHERE rejection_reason IS NOT NULL
        """
    )


def run(config: Config, con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyConnection:
    """Build Silver from Bronze and write it to Parquet. Returns the connection for downstream steps."""
    con = con or duckdb.connect()
    bronze_glob = str(config.paths.bronze / "ar_open_items" / "**" / "*.parquet")
    build(con, bronze_glob, config.aging_buckets)

    config.paths.silver.mkdir(parents=True, exist_ok=True)
    for table, file_name in (("silver_open_items", "ar_open_items"), ("silver_rejected", "rejected_records")):
        target: Path = config.paths.silver / f"{file_name}.parquet"
        con.execute(f"COPY {table} TO '{target}' (FORMAT parquet)")

    accepted = con.execute("SELECT count(*) FROM silver_open_items").fetchall()[0][0]
    rejected = con.execute("SELECT count(*) FROM silver_rejected").fetchall()[0][0]
    log.info("Silver: %s accepted rows, %s rejected rows", f"{accepted:,}", f"{rejected:,}")
    return con
