"""Bronze layer: land raw SAP exports as Parquet without changing their content.

Rules:
* Every column is kept as text; typing happens in Silver, so nothing is lost on ingestion.
* Column names are normalised to snake_case for SQL-friendly downstream use.
* Lineage columns are added (source file, ingestion timestamp).
* Files that do not match the expected name or schema go to quarantine instead of failing the run.
* A snapshot that already exists in Bronze is never overwritten unless ``force=True``.
"""

from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from ar_pipeline.config import Config

log = logging.getLogger(__name__)

FILENAME_PATTERN = re.compile(r"^AR_OPEN_ITEMS_(?P<company>\d{4})_(?P<date>\d{8})\.csv$")

# Raw SAP header -> Bronze column name
COLUMN_MAP = {
    "Company Code": "company_code",
    "Customer": "customer_id",
    "Customer Name": "customer_name",
    "Segment": "segment",
    "Document Number": "document_number",
    "Document Type": "document_type",
    "Business Area": "business_area",
    "Posting Date": "posting_date",
    "Net Due Date": "net_due_date",
    "Amount in LC": "amount_lc",
    "Currency": "currency",
    "Days in Arrears": "days_in_arrears",
    "Collector": "collector",
}


@dataclass
class IngestionResult:
    file: str
    status: str  # loaded | skipped_existing | quarantined
    rows: int = 0
    detail: str = ""


def bronze_table_dir(config: Config) -> Path:
    return config.paths.bronze / "ar_open_items"


def _quarantine(path: Path, config: Config, reason: str) -> IngestionResult:
    quarantine_dir = config.paths.landing.parent / "quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, quarantine_dir / path.name)
    log.warning("Quarantined %s: %s", path.name, reason)
    return IngestionResult(path.name, "quarantined", detail=reason)


def ingest_file(con: duckdb.DuckDBPyConnection, path: Path, config: Config, force: bool = False) -> IngestionResult:
    """Ingest one landing file into its Bronze partition."""
    match = FILENAME_PATTERN.match(path.name)
    if not match:
        return _quarantine(path, config, "file name does not match AR_OPEN_ITEMS_<company>_<yyyymmdd>.csv")

    snapshot = datetime.strptime(match["date"], "%Y%m%d").date()
    target = (
        bronze_table_dir(config)
        / f"snapshot_date={snapshot:%Y-%m-%d}"
        / f"company_code={match['company']}"
        / "data.parquet"
    )
    if target.exists() and not force:
        return IngestionResult(path.name, "skipped_existing")

    raw = con.read_csv(str(path), delimiter=";", header=True, all_varchar=True)
    missing = set(COLUMN_MAP) - set(raw.columns)
    if missing:
        return _quarantine(path, config, f"missing columns: {sorted(missing)}")

    # company_code and snapshot_date come from the partition path (file name). The company code
    # written inside the file is kept as source_company_code so Silver can check they agree.
    select_list = ", ".join(
        f'"{source}" AS {"source_company_code" if target_name == "company_code" else target_name}'
        for source, target_name in COLUMN_MAP.items()
    )
    con.register("raw_file", raw)
    relation = con.sql(
        f"""
        SELECT
            {select_list},
            '{path.name}' AS _source_file,
            current_timestamp AS _ingested_at
        FROM raw_file
        """
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    relation.write_parquet(str(target))
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchall()[0][0]
    con.unregister("raw_file")
    return IngestionResult(path.name, "loaded", rows=rows)


def run(config: Config, force: bool = False) -> list[IngestionResult]:
    """Ingest every landing file. Company code and snapshot date are stored as Hive partitions."""
    con = duckdb.connect()
    results = [ingest_file(con, path, config, force) for path in sorted(config.paths.landing.glob("*.csv"))]
    loaded = sum(r.status == "loaded" for r in results)
    log.info(
        "Bronze: %s loaded, %s skipped (already ingested), %s quarantined",
        loaded,
        sum(r.status == "skipped_existing" for r in results),
        sum(r.status == "quarantined" for r in results),
    )
    return results
