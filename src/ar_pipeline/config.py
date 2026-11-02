"""Load pipeline configuration and resolve project paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class Paths:
    landing: Path
    bronze: Path
    silver: Path
    gold: Path
    reports: Path
    images: Path

    def ensure(self) -> None:
        for path in (self.landing, self.bronze, self.silver, self.gold, self.reports, self.images):
            path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Config:
    raw: dict[str, Any]
    paths: Paths

    @property
    def synthetic(self) -> dict[str, Any]:
        return self.raw["synthetic_data"]

    @property
    def companies(self) -> list[dict[str, str]]:
        return self.raw["companies"]

    @property
    def business_areas(self) -> dict[str, str]:
        return self.raw["business_areas"]

    @property
    def aging_buckets(self) -> list[dict[str, Any]]:
        return self.raw["aging_buckets"]

    @property
    def overdue_change_threshold(self) -> float:
        return float(self.raw["customer_trend"]["overdue_change_threshold"])


def load_config(config_path: Path | str = DEFAULT_CONFIG, root: Path | str | None = None) -> Config:
    """Read the YAML config. Relative paths are resolved against ``root`` (project root by default)."""
    config_path = Path(config_path)
    base = Path(root) if root is not None else PROJECT_ROOT
    with config_path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    paths = Paths(**{name: (base / value).resolve() for name, value in raw["paths"].items()})
    return Config(raw=raw, paths=paths)
