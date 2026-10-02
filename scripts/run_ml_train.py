"""Train and evaluate the Week 4 hourly-demand model (Tasks 2 and 3)."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_platform.ml.train import train_and_evaluate
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="week4")
    args = parser.parse_args()
    cfg = load_config(str(ROOT / "config/datasets.yaml"))
    spark = get_spark("week4-ml-train")
    try:
        print(json.dumps(train_and_evaluate(spark, cfg, run_id=args.run_id), indent=2, default=str), flush=True)
    finally:
        spark.stop()
