"""Tests for the SQL Server loader that do not need a running SQL Server.

A fake connection stands in for pyodbc so the transactional logic (delete, insert,
reconcile, commit or rollback) can be tested anywhere, including CI.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ar_pipeline import bronze, generate, gold, load_sqlserver as ls, silver
from ar_pipeline.config import Config

DDL = (ls.SQL_DIR / "02_create_gold_tables.sql").read_text(encoding="utf-8")


class FakeCursor:
    """Keeps inserted rows in memory and answers the reconciliation queries."""

    def __init__(self, drop_rows_from: str | None = None) -> None:
        self.tables: dict[str, list[tuple]] = {}
        self.drop_rows_from = drop_rows_from
        self.fast_executemany = False
        self._result: tuple | None = None

    def execute(self, sql: str, *params):
        if match := re.match(r"DELETE FROM gold\.(\w+)", sql):
            self.tables[match[1]] = []
        elif match := re.match(r"SELECT COUNT\(\*\) FROM gold\.(\w+)", sql):
            self._result = (len(self.tables[match[1]]),)
        elif match := re.match(r"SELECT COALESCE\(SUM\((\w+)\), 0\) FROM gold\.(\w+)", sql):
            column, table = match[1], match[2]
            index = ls.TABLE_COLUMNS[table].index(column)
            self._result = (sum(row[index] for row in self.tables[table]),)
        return self

    def executemany(self, sql: str, rows):
        table = re.match(r"INSERT INTO gold\.(\w+)", sql)[1]
        rows = list(rows)
        if table == self.drop_rows_from:
            rows = rows[:-1]  # simulate a silently lost row
        self.tables.setdefault(table, []).extend(rows)

    def fetchone(self):
        return self._result


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


@pytest.fixture
def gold_dir(config: Config) -> Path:
    generate.generate(config)
    bronze.run(config)
    con = silver.run(config)
    gold.run(con, config)
    return config.paths.gold


def test_load_commits_when_everything_reconciles(gold_dir: Path) -> None:
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    counts = ls.load(conn, gold_dir, batch_size=500)
    assert conn.committed and not conn.rolled_back
    assert counts["fact_ar_open_items"] == len(cursor.tables["fact_ar_open_items"]) > 0
    assert cursor.fast_executemany is True


def test_load_rolls_back_when_rows_are_lost(gold_dir: Path) -> None:
    conn = FakeConnection(FakeCursor(drop_rows_from="fact_ar_open_items"))
    with pytest.raises(ls.LoadError, match="fact_ar_open_items"):
        ls.load(conn, gold_dir)
    assert conn.rolled_back and not conn.committed


def test_loader_columns_match_tsql_ddl() -> None:
    """The INSERT column lists must match CREATE TABLE exactly, in the same order."""
    for table, columns in ls.TABLE_COLUMNS.items():
        block = re.search(rf"CREATE TABLE gold\.{table} \((.*?)\n\);", DDL, flags=re.S)
        assert block, f"gold.{table} missing from DDL"
        ddl_columns = [
            line.split()[0].strip("[]")
            for line in block[1].strip().splitlines()
            if line.strip() and not line.strip().startswith(("CONSTRAINT", "--"))
        ]
        assert ddl_columns == columns, table


def test_load_order_respects_foreign_keys() -> None:
    order = list(ls.TABLE_COLUMNS)
    for match in re.finditer(r"CREATE TABLE gold\.(\w+) \((.*?)\n\);", DDL, flags=re.S):
        for parent in re.findall(r"REFERENCES gold\.(\w+)", match[2]):
            assert order.index(parent) < order.index(match[1]), f"{parent} must load before {match[1]}"


def test_split_batches_handles_go_separators() -> None:
    script = "SELECT 1;\nGO\n\n  go  \nSELECT 2;\nSELECT 'GO' AS text;\nGO;\n"
    assert ls.split_batches(script) == ["SELECT 1;", "SELECT 2;\nSELECT 'GO' AS text;"]


def test_connection_string_windows_auth_by_default(config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("AR_SQL_SERVER", "AR_SQL_USER", "AR_SQL_PASSWORD", "AR_SQL_DATABASE", "AR_SQL_DRIVER"):
        monkeypatch.delenv(var, raising=False)
    conn_str = ls.connection_string(ls.sql_settings(config))
    assert "Trusted_Connection=yes" in conn_str
    assert "DATABASE=ARAnalytics" in conn_str
    assert "UID=" not in conn_str


def test_connection_string_uses_environment_overrides(config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AR_SQL_SERVER", r"localhost\SQLEXPRESS")
    monkeypatch.setenv("AR_SQL_USER", "etl_user")
    monkeypatch.setenv("AR_SQL_PASSWORD", "from-env")
    conn_str = ls.connection_string(ls.sql_settings(config), database="master")
    assert r"SERVER=localhost\SQLEXPRESS" in conn_str
    assert "DATABASE=master" in conn_str
    assert "UID=etl_user" in conn_str and "Trusted_Connection" not in conn_str


def test_tsql_scripts_parse() -> None:
    sqlglot = pytest.importorskip("sqlglot")
    for path in sorted(ls.SQL_DIR.glob("*.sql")):
        for batch in ls.split_batches(path.read_text(encoding="utf-8")):
            if "EXEC (" in batch:  # dynamic EXEC() is valid T-SQL but not supported by the parser
                continue
            sqlglot.parse(batch, read="tsql")


def test_tsql_block_comments_are_balanced_and_not_nested() -> None:
    """SQL Server nests /* */ comments, so a stray '/*' inside a comment (e.g. a glob like
    data/*.parquet) silently swallows the rest of the script."""
    outside = re.compile(r"/\*|'(?:''|[^'])*'|--[^\n]*")  # outside comments, strings and -- hide markers
    inside = re.compile(r"/\*|\*/")  # inside a block comment only these two matter
    for path in sorted(ls.SQL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        depth = max_depth = position = 0
        while True:
            match = (inside if depth else outside).search(text, position)
            if not match:
                break
            position = match.end()
            token = match.group()
            if token == "/*":
                depth += 1
            elif token == "*/":
                depth -= 1
            max_depth = max(max_depth, depth)
        assert depth == 0, f"{path.name}: unclosed block comment"
        assert max_depth == 1, f"{path.name}: nested block comment"
