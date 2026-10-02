"""Build (and persist) the Task 1 training dataset; print split sizes."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_platform.ml.dataset import persist_training_dataset
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path)
    args = parser.parse_args()
    cfg = load_config(str(ROOT / "config/datasets.yaml"))
    if args.raw_dir:
        cfg.raw_data_dir = str(args.raw_dir.resolve())
    spark = get_spark("week4-ml-dataset")
    try:
        print(json.dumps(persist_training_dataset(spark, cfg), indent=2, default=str), flush=True)
    finally:
        spark.stop()
