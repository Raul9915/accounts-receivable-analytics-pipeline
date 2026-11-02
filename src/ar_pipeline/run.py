"""Run the full pipeline: (generate) -> Bronze -> Silver -> Gold -> quality -> analytics.

Usage:
    python -m ar_pipeline.run              # generate data if landing is empty, then run all layers
    python -m ar_pipeline.run --regenerate # rebuild synthetic data and re-ingest everything
"""

from __future__ import annotations

import argparse
import logging
import shutil
import time

from ar_pipeline import analytics, bronze, generate, gold, quality, silver
from ar_pipeline.config import DEFAULT_CONFIG, load_config

log = logging.getLogger("ar_pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Accounts Receivable Medallion pipeline")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to pipeline.yaml")
    parser.add_argument("--regenerate", action="store_true", help="Regenerate synthetic data and rebuild all layers")
    parser.add_argument("--skip-charts", action="store_true", help="Do not render README charts")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    config = load_config(args.config)
    started = time.perf_counter()

    if args.regenerate:
        for path in (config.paths.landing, config.paths.bronze, config.paths.silver, config.paths.gold):
            shutil.rmtree(path, ignore_errors=True)
    config.paths.ensure()

    if not any(config.paths.landing.glob("*.csv")):
        generate.generate(config)

    bronze.run(config, force=args.regenerate)
    con = silver.run(config)
    gold.run(con, config)
    quality.run(con, config)
    analytics.run(con, config, render_charts=not args.skip_charts)

    log.info("Pipeline finished in %.1f s", time.perf_counter() - started)


if __name__ == "__main__":
    main()
