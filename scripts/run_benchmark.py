"""CLI entry point: run the Task 6 storage-strategy benchmark and save results."""
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from urban_platform.benchmark.storage_benchmark import benchmark_strategy
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

CONFIG_PATH = str(Path(__file__).resolve().parent.parent / "config" / "datasets.yaml")


def main() -> None:
    platform_cfg = load_config(CONFIG_PATH)
    spark = get_spark(app_name="urban-platform-benchmark")

    results = []
    results.append(benchmark_strategy(spark, platform_cfg, "by_month", ["pickup_year", "pickup_month"]))
    results.append(benchmark_strategy(spark, platform_cfg, "by_borough", ["pickup_borough"]))

    out_path = Path(platform_cfg.metadata_dir) / "benchmark_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2)

    print(f"Benchmark results written to {out_path}")
    for r in results:
        print(f"\n=== {r.strategy} (partition_by={r.partition_by}) ===")
        print(f"  ingestion_seconds: {r.ingestion_seconds}")
        print(f"  storage_bytes: {r.storage_bytes} ({r.storage_bytes/1024/1024:.1f} MB)")
        print(f"  file_count: {r.file_count}")
        for q, lat in r.query_latencies_seconds.items():
            print(f"  query[{q}]: {lat}s")

    spark.stop()


if __name__ == "__main__":
    main()