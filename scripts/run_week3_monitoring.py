"""Run the four Week 3 operational Spark SQL reports against Delta events."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_platform.monitoring.events import run_table_path
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark


def main() -> None:
    spark = get_spark("week3-operational-monitoring")
    try:
        cfg = load_config(str(ROOT / "config/datasets.yaml"))
        spark.read.format("delta").load(run_table_path(cfg)).createOrReplaceTempView(
            "monitoring_pipeline_runs"
        )
        output = ROOT / "artifacts/week3/monitoring"
        output.mkdir(parents=True, exist_ok=True)
        for source in sorted((ROOT / "sql/monitoring").glob("*.sql")):
            rows = spark.sql(source.read_text(encoding="utf-8")).collect()
            target = output / f"{source.stem}.json"
            target.write_text(json.dumps([r.asDict() for r in rows], indent=2, default=str),
                              encoding="utf-8")
            print(f"{source.stem}: {len(rows)} rows -> {target}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
