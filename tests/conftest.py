from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ar_pipeline.config import DEFAULT_CONFIG, Config, load_config

HEADER = (
    "Company Code;Customer;Customer Name;Segment;Document Number;Document Type;Business Area;"
    "Posting Date;Net Due Date;Amount in LC;Currency;Days in Arrears;Collector"
)


@pytest.fixture
def config(tmp_path: Path) -> Config:
    """A copy of the real config whose paths point to a temporary folder and uses a small dataset."""
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text(encoding="utf-8"))
    raw["synthetic_data"]["n_customers"] = 80
    raw["synthetic_data"]["n_snapshots"] = 3
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    cfg = load_config(config_path, root=tmp_path)
    cfg.paths.ensure()
    return cfg


def write_landing(config: Config, name: str, rows: list[str]) -> Path:
    path = config.paths.landing / name
    path.write_text("\n".join([HEADER, *rows]) + "\n", encoding="utf-8")
    return path
