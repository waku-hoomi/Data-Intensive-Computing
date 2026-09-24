"""Validate the latest published Week 3 snapshot and write auditable JSON."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from delta.tables import DeltaTable

from urban_platform.analytics.context import QUERY_IDS, register_analysis_views, sql_text
from urban_platform.analytics.products import PRODUCTS, product_path
from urban_platform.monitoring.publication import latest_publication
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main() -> None:
    cfg = load_config(str(ROOT / "config/datasets.yaml"))
    manifest = json.loads((ROOT / "data/updates/manifest.json").read_text(encoding="utf-8"))
    file_checks = {}
    for name, record in manifest["datasets"].items():
        path = ROOT / "data/updates" / record["file"]
        actual = digest(path)
        if actual != record["sha256"]:
            raise AssertionError(f"Update hash mismatch for {name}")
        file_checks[name] = actual

    spark = get_spark("week3-final-validation")
    try:
        publication = latest_publication(spark, cfg)
        if publication is None:
            raise AssertionError("No published Week 3 snapshot")
        gold_path = str(Path(cfg.gold_dir) / "integrated_taxi_trips")
        gold = (spark.read.format("delta").option("versionAsOf", publication["gold_version"])
                .load(gold_path))
        taxi = (spark.read.format("delta")
                .option("versionAsOf", publication["bronze_versions"]["taxi_trips"])
                .load(str(Path(cfg.bronze_dir) / "taxi_trips")))
        gold_rows = gold.count()
        taxi_rows = taxi.count()
        if gold_rows != taxi_rows:
            raise AssertionError(f"Gold/taxi row mismatch: {gold_rows} != {taxi_rows}")

        product_rows = {}
        for name in PRODUCTS:
            frame = (spark.read.format("delta")
                     .option("versionAsOf", publication["product_versions"][name])
                     .load(str(product_path(name))))
            product_rows[name] = frame.count()

        register_analysis_views(spark, gold, publication["analysis_config"])
        query_rows = {name: spark.sql(sql_text(name)).count() for name in QUERY_IDS}
        monthly = spark.sql(sql_text("06_monthly_trend")).orderBy("local_month").collect()
        if monthly and not monthly[-1]["is_complete_month"]:
            if monthly[-1]["month_over_month_pct"] is not None:
                raise AssertionError("Incomplete month must have NULL month-over-month total")

        current_gold = int(DeltaTable.forPath(spark, gold_path).history(1).first().version)
        result = {
            "status": "passed", "run_id": publication["run_id"],
            "publication_gold_version": publication["gold_version"],
            "current_gold_version": current_gold,
            "gold_rows": gold_rows, "bronze_taxi_rows": taxi_rows,
            "product_rows": product_rows, "query_rows": query_rows,
            "analysis_config": publication["analysis_config"],
            "last_month": monthly[-1].asDict() if monthly else None,
            "update_sha256": file_checks,
        }
        output = ROOT / "artifacts/week3/final_validation.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        print(json.dumps(result, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
