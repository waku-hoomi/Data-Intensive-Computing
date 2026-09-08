"""CLI entry point: run ingestion for one dataset or all datasets.

Usage:
  python scripts/run_ingestion.py                # ingest everything
  python scripts/run_ingestion.py taxi_trips      # ingest one dataset
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from urban_platform.ingestion.pipeline import ingest_all, ingest_dataset
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

CONFIG_PATH = str(Path(__file__).resolve().parent.parent / "config" / "datasets.yaml")


def main() -> None:
    platform_cfg = load_config(CONFIG_PATH)
    spark = get_spark(app_name="urban-platform-ingestion")

    if len(sys.argv) > 1:
        dataset_name = sys.argv[1]
        record = ingest_dataset(spark, platform_cfg, dataset_name)
        _print_record(record)
    else:
        records = ingest_all(spark, platform_cfg)
        for record in records:
            _print_record(record)

    spark.stop()


def _print_record(record) -> None:
    print(
        f"[{record.status.upper()}] {record.dataset_name}: "
        f"input={record.input_rows} output={record.output_rows} rejected={record.rejected_rows} "
        f"({record.execution_seconds}s) -> {record.output_path}"
    )
    if record.rejected_by_reason:
        print(f"    rejected_by_reason={record.rejected_by_reason}")


if __name__ == "__main__":
    main()