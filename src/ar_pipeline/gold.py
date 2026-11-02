"""Gold layer: star schema and analytical tables ready for Power BI.

Star schema (grain: one open document per month-end snapshot)
    fact_ar_open_items -> dim_date, dim_company, dim_customer, dim_business_area, dim_aging_bucket

Analytical tables
    agg_portfolio_monthly   balance by snapshot, company, business area and aging bucket
    customer_monthly        one row per customer and snapshot, with worst bucket and trend status
    bucket_transitions      how customers move between worst buckets month over month (roll rates)
"""

from __future__ import annotations

import logging

import duckdb

from ar_pipeline.config import Config

log = logging.getLogger(__name__)

GOLD_TABLES = [
    "dim_date",
    "dim_company",
    "dim_business_area",
    "dim_aging_bucket",
    "dim_customer",
    "fact_ar_open_items",
    "agg_portfolio_monthly",
    "customer_monthly",
    "bucket_transitions",
]


def _values_table(con: duckdb.DuckDBPyConnection, name: str, columns: list[str], rows: list[tuple]) -> None:
    placeholders = ", ".join("(" + ", ".join("?" for _ in columns) + ")" for _ in rows)
    params = [value for row in rows for value in row]
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM (VALUES {placeholders}) AS t({', '.join(columns)})", params)


def build_dimensions(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    _values_table(con, "dim_company", ["company_code", "company_name"], [(c["code"], c["name"]) for c in config.companies])
    _values_table(
        con, "dim_business_area", ["business_area", "business_area_name"], list(config.business_areas.items())
    )
    _values_table(
        con,
        "dim_aging_bucket",
        ["aging_bucket_id", "aging_bucket", "is_overdue_bucket"],
        [(b["id"], b["label"], b["id"] > 1) for b in config.aging_buckets],
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_date AS
        SELECT DISTINCT
            snapshot_date,
            year(snapshot_date)                       AS year,
            month(snapshot_date)                      AS month,
            strftime(snapshot_date, '%Y-%m')          AS year_month,
            strftime(snapshot_date, '%b %Y')          AS month_label
        FROM silver_open_items
        ORDER BY snapshot_date
        """
    )
    # Slowly changing attributes: keep the latest known value, plus first/last appearance.
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_customer AS
        SELECT
            customer_id,
            arg_max(customer_name, snapshot_date)     AS customer_name,
            arg_max(segment, snapshot_date)           AS segment,
            arg_max(collector, snapshot_date)         AS collector,
            min(snapshot_date)                        AS first_snapshot,
            max(snapshot_date)                        AS last_snapshot
        FROM silver_open_items
        GROUP BY customer_id
        ORDER BY customer_id
        """
    )


def build_fact(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE fact_ar_open_items AS
        SELECT
            snapshot_date, company_code, customer_id, business_area, aging_bucket_id,
            document_number, document_type, posting_date, due_date,
            amount_lc, days_in_arrears, is_overdue
        FROM silver_open_items
        """
    )


def build_analytics(con: duckdb.DuckDBPyConnection, change_threshold: float) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE agg_portfolio_monthly AS
        SELECT
            snapshot_date, company_code, business_area, aging_bucket_id,
            sum(amount_lc)                 AS balance_lc,
            count(*)                       AS documents,
            count(DISTINCT customer_id)    AS customers
        FROM fact_ar_open_items
        GROUP BY ALL
        ORDER BY ALL
        """
    )

    # Credit notes (negative amounts) reduce balances but do not define how late a customer is.
    con.execute(
        """
        CREATE OR REPLACE TABLE customer_snapshot AS
        SELECT
            snapshot_date,
            customer_id,
            sum(amount_lc)                                                    AS total_balance_lc,
            sum(amount_lc) FILTER (WHERE is_overdue)                          AS overdue_balance_raw,
            coalesce(max(aging_bucket_id) FILTER (WHERE amount_lc > 0), 1)    AS worst_bucket_id,
            coalesce(max(days_in_arrears) FILTER (WHERE amount_lc > 0), 0)    AS max_days_in_arrears,
            count(*)                                                          AS open_documents
        FROM fact_ar_open_items
        GROUP BY snapshot_date, customer_id
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE snapshot_calendar AS
        SELECT snapshot_date, lag(snapshot_date) OVER (ORDER BY snapshot_date) AS previous_snapshot
        FROM dim_date
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE customer_monthly AS
        WITH current_month AS (
            SELECT
                c.*,
                coalesce(c.overdue_balance_raw, 0) AS overdue_balance_lc,
                cal.previous_snapshot
            FROM customer_snapshot c
            JOIN snapshot_calendar cal USING (snapshot_date)
        )
        SELECT
            cur.snapshot_date,
            cur.customer_id,
            cur.total_balance_lc,
            cur.overdue_balance_lc,
            CASE WHEN cur.total_balance_lc > 0
                 THEN round(cur.overdue_balance_lc / cur.total_balance_lc, 4) END       AS overdue_ratio,
            cur.worst_bucket_id,
            cur.max_days_in_arrears,
            cur.open_documents,
            prev.worst_bucket_id                                                         AS previous_worst_bucket_id,
            coalesce(prev.overdue_balance_raw, 0)                                        AS previous_overdue_balance_lc,
            CASE
                WHEN cur.previous_snapshot IS NULL THEN 'Baseline'
                WHEN prev.customer_id IS NULL THEN 'New'
                WHEN cur.worst_bucket_id > prev.worst_bucket_id THEN 'Deteriorated'
                WHEN cur.worst_bucket_id < prev.worst_bucket_id THEN 'Improved'
                WHEN coalesce(prev.overdue_balance_raw, 0) = 0 AND cur.overdue_balance_lc > 0 THEN 'Deteriorated'
                WHEN coalesce(prev.overdue_balance_raw, 0) = 0 THEN 'Stable'
                WHEN (cur.overdue_balance_lc - prev.overdue_balance_raw) / prev.overdue_balance_raw > {change_threshold}
                    THEN 'Deteriorated'
                WHEN (cur.overdue_balance_lc - prev.overdue_balance_raw) / prev.overdue_balance_raw < -{change_threshold}
                    THEN 'Improved'
                ELSE 'Stable'
            END                                                                          AS trend_status
        FROM current_month cur
        LEFT JOIN customer_snapshot prev
            ON prev.customer_id = cur.customer_id
           AND prev.snapshot_date = cur.previous_snapshot
        ORDER BY cur.snapshot_date, cur.customer_id
        """
    )

    # Roll rates: where did each customer's worst bucket go one month later?
    # to_bucket_id = 0 means the customer no longer has open items (fully settled).
    con.execute(
        """
        CREATE OR REPLACE TABLE bucket_transitions AS
        SELECT
            cal.snapshot_date                                 AS to_snapshot,
            prev.snapshot_date                                AS from_snapshot,
            prev.worst_bucket_id                              AS from_bucket_id,
            coalesce(cur.worst_bucket_id, 0)                  AS to_bucket_id,
            count(*)                                          AS customers,
            sum(prev.total_balance_lc)                        AS from_balance_lc
        FROM snapshot_calendar cal
        JOIN customer_snapshot prev ON prev.snapshot_date = cal.previous_snapshot
        LEFT JOIN customer_snapshot cur
            ON cur.customer_id = prev.customer_id
           AND cur.snapshot_date = cal.snapshot_date
        GROUP BY ALL
        ORDER BY ALL
        """
    )
    con.execute("DROP TABLE customer_snapshot")
    con.execute("DROP TABLE snapshot_calendar")


def run(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """Build all Gold tables from ``silver_open_items`` (already in ``con``) and write them to Parquet."""
    build_dimensions(con, config)
    build_fact(con)
    build_analytics(con, config.overdue_change_threshold)

    config.paths.gold.mkdir(parents=True, exist_ok=True)
    for table in GOLD_TABLES:
        con.execute(f"COPY {table} TO '{config.paths.gold / f'{table}.parquet'}' (FORMAT parquet)")
    log.info("Gold: wrote %s tables to %s", len(GOLD_TABLES), config.paths.gold)
