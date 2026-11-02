"""Publish the Gold layer to SQL Server (serving layer for Power BI).

Parquet files in ``data/gold`` remain the source of truth. This step copies them into the
``gold`` schema of a SQL Server database with primary keys, foreign keys and check constraints,
so the model is enforced by the database as well as by the pipeline.

The load is a **full refresh inside one transaction**: either every table is replaced and
reconciles with the Parquet files, or nothing changes.

Usage (Windows, from the project root):
    python -m ar_pipeline.load_sqlserver --init   # first time: create database, tables and views
    python -m ar_pipeline.load_sqlserver          # reload data after each pipeline run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Protocol

import duckdb

from ar_pipeline.config import DEFAULT_CONFIG, PROJECT_ROOT, Config, load_config

log = logging.getLogger(__name__)

SQL_DIR = PROJECT_ROOT / "sql"
INIT_SCRIPTS_MASTER = ["01_create_database.sql"]
INIT_SCRIPTS_DATABASE = ["02_create_gold_tables.sql", "03_create_views.sql"]

# Load order respects foreign keys (dimensions first). Column order matches sql/02_create_gold_tables.sql.
TABLE_COLUMNS: dict[str, list[str]] = {
    "dim_company": ["company_code", "company_name"],
    "dim_business_area": ["business_area", "business_area_name"],
    "dim_aging_bucket": ["aging_bucket_id", "aging_bucket", "is_overdue_bucket"],
    "dim_date": ["snapshot_date", "year", "month", "year_month", "month_label"],
    "dim_customer": ["customer_id", "customer_name", "segment", "collector", "first_snapshot", "last_snapshot"],
    "fact_ar_open_items": [
        "snapshot_date", "company_code", "customer_id", "business_area", "aging_bucket_id", "document_number",
        "document_type", "posting_date", "due_date", "amount_lc", "days_in_arrears", "is_overdue",
    ],
    "agg_portfolio_monthly": [
        "snapshot_date", "company_code", "business_area", "aging_bucket_id", "balance_lc", "documents", "customers",
    ],
    "customer_monthly": [
        "snapshot_date", "customer_id", "total_balance_lc", "overdue_balance_lc", "overdue_ratio", "worst_bucket_id",
        "max_days_in_arrears", "open_documents", "previous_worst_bucket_id", "previous_overdue_balance_lc",
        "trend_status",
    ],
    "bucket_transitions": [
        "to_snapshot", "from_snapshot", "from_bucket_id", "to_bucket_id", "customers", "from_balance_lc",
    ],
}

# Amount column reconciled per table (sum in SQL Server must equal sum in Parquet)
AMOUNT_COLUMNS = {
    "fact_ar_open_items": "amount_lc",
    "agg_portfolio_monthly": "balance_lc",
    "customer_monthly": "total_balance_lc",
}


class LoadError(RuntimeError):
    """Raised when the data in SQL Server does not reconcile with the Gold Parquet files."""


class Cursor(Protocol):
    fast_executemany: bool

    def execute(self, sql: str, *params: Any) -> Any: ...
    def executemany(self, sql: str, rows: Iterable[tuple]) -> Any: ...
    def fetchone(self) -> Any: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


# ----------------------------------------------------------------------------------------------
# Connection
# ----------------------------------------------------------------------------------------------

def sql_settings(config: Config) -> dict[str, Any]:
    """Merge config values with environment-variable overrides."""
    settings = dict(config.raw.get("sqlserver", {}))
    overrides = {
        "server": "AR_SQL_SERVER",
        "database": "AR_SQL_DATABASE",
        "driver": "AR_SQL_DRIVER",
        "user": "AR_SQL_USER",
        "password": "AR_SQL_PASSWORD",
    }
    for key, env_var in overrides.items():
        if os.environ.get(env_var):
            settings[key] = os.environ[env_var]
    return settings


def connection_string(settings: dict[str, Any], database: str | None = None) -> str:
    """Build an ODBC connection string. Uses Windows authentication unless a user is provided."""
    parts = [
        f"DRIVER={{{settings['driver']}}}",
        f"SERVER={settings['server']}",
        f"DATABASE={database or settings['database']}",
    ]
    if settings.get("user"):
        parts += [f"UID={settings['user']}", f"PWD={settings.get('password', '')}"]
    else:
        parts.append("Trusted_Connection=yes")
    if settings.get("trust_server_certificate", True):
        parts.append("TrustServerCertificate=yes")
    return ";".join(parts) + ";"


def _connect(conn_str: str, autocommit: bool):  # pragma: no cover - needs a real SQL Server
    try:
        import pyodbc
    except ImportError as exc:
        raise SystemExit(
            "pyodbc is not installed. Run: pip install -r requirements-sqlserver.txt "
            "(and install 'ODBC Driver 18 for SQL Server' from Microsoft)."
        ) from exc
    return pyodbc.connect(conn_str, autocommit=autocommit)


# ----------------------------------------------------------------------------------------------
# T-SQL scripts
# ----------------------------------------------------------------------------------------------

def split_batches(script: str) -> list[str]:
    """Split a T-SQL script on GO separators (as SSMS/sqlcmd do) and drop empty batches."""
    batches = re.split(r"^\s*GO\s*;?\s*$", script, flags=re.IGNORECASE | re.MULTILINE)
    return [batch.strip() for batch in batches if batch.strip()]


def run_script(conn: Connection, path: Path) -> None:
    cursor = conn.cursor()
    for batch in split_batches(path.read_text(encoding="utf-8-sig")):
        cursor.execute(batch)
    log.info("Executed %s", path.name)


# ----------------------------------------------------------------------------------------------
# Load
# ----------------------------------------------------------------------------------------------

def read_gold_rows(gold_dir: Path, table: str) -> list[tuple]:
    """Read a Gold Parquet table as Python tuples in the column order of the SQL table."""
    columns = ", ".join(f'"{column}"' for column in TABLE_COLUMNS[table])
    path = gold_dir / f"{table}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run `python -m ar_pipeline.run` first.")
    return duckdb.sql(f"SELECT {columns} FROM read_parquet('{path.as_posix()}')").fetchall()


def insert_sql(table: str) -> str:
    columns = TABLE_COLUMNS[table]
    column_list = ", ".join(f"[{column}]" for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO gold.{table} ({column_list}) VALUES ({placeholders})"


def _chunks(rows: list[tuple], size: int) -> Iterable[list[tuple]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def load(conn: Connection, gold_dir: Path, batch_size: int = 5000) -> dict[str, int]:
    """Replace every Gold table in SQL Server and reconcile it. Commits only if all checks pass."""
    data = {table: read_gold_rows(gold_dir, table) for table in TABLE_COLUMNS}
    cursor = conn.cursor()
    cursor.fast_executemany = True
    try:
        for table in reversed(list(TABLE_COLUMNS)):  # children first because of foreign keys
            cursor.execute(f"DELETE FROM gold.{table}")
        for table, rows in data.items():
            statement = insert_sql(table)
            for chunk in _chunks(rows, batch_size):
                cursor.executemany(statement, chunk)
            log.info("Loaded gold.%s: %s rows", table, f"{len(rows):,}")
        reconcile(cursor, data)
    except Exception:
        conn.rollback()
        log.error("SQL Server load rolled back; the database was left unchanged")
        raise
    conn.commit()
    return {table: len(rows) for table, rows in data.items()}


def reconcile(cursor: Cursor, data: dict[str, list[tuple]]) -> None:
    """Compare row counts and amounts in SQL Server against the Parquet source."""
    problems = []
    for table, rows in data.items():
        db_rows = cursor.execute(f"SELECT COUNT(*) FROM gold.{table}").fetchone()[0]
        if db_rows != len(rows):
            problems.append(f"{table}: {db_rows} rows in SQL Server vs {len(rows)} in Parquet")
        if table in AMOUNT_COLUMNS:
            column = AMOUNT_COLUMNS[table]
            index = TABLE_COLUMNS[table].index(column)
            expected = sum((Decimal(str(row[index])) for row in rows), Decimal("0"))
            actual = cursor.execute(f"SELECT COALESCE(SUM({column}), 0) FROM gold.{table}").fetchone()[0]
            if abs(Decimal(str(actual)) - expected) > Decimal("0.01"):
                problems.append(f"{table}.{column}: {actual} in SQL Server vs {expected} in Parquet")
    if problems:
        raise LoadError("SQL Server reconciliation failed: " + "; ".join(problems))
    log.info("SQL Server reconciliation passed for %s tables", len(data))


def main() -> None:  # pragma: no cover - needs a real SQL Server
    parser = argparse.ArgumentParser(description="Load the Gold layer into SQL Server")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to pipeline.yaml")
    parser.add_argument("--init", action="store_true", help="Create the database, gold tables and views first")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    config = load_config(args.config)
    settings = sql_settings(config)
    log.info("Target: %s / %s", settings["server"], settings["database"])

    if args.init:
        master = _connect(connection_string(settings, database="master"), autocommit=True)
        for script in INIT_SCRIPTS_MASTER:
            run_script(master, SQL_DIR / script)
        master.close()
        ddl = _connect(connection_string(settings), autocommit=True)
        for script in INIT_SCRIPTS_DATABASE:
            run_script(ddl, SQL_DIR / script)
        ddl.close()

    conn = _connect(connection_string(settings), autocommit=False)
    try:
        counts = load(conn, config.paths.gold, batch_size=int(settings.get("batch_size", 5000)))
    finally:
        conn.close()
    log.info("Done: %s rows loaded into %s", f"{sum(counts.values()):,}", settings["database"])


if __name__ == "__main__":
    main()
