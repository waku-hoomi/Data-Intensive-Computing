"""Create a strict baseline or apply and publish one Week 3 update release."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_platform.incremental.runner import bootstrap_platform, process_updates
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--updates-dir", type=Path, default=ROOT / "data/updates")
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    cfg = load_config(str(ROOT / "config/datasets.yaml"))
    if args.raw_dir:
        cfg.raw_data_dir = str(args.raw_dir.resolve())
    spark = get_spark("week3-incremental-platform")
    try:
        result = (bootstrap_platform(spark, cfg, args.run_id) if args.bootstrap else
                  process_updates(spark, cfg, args.updates_dir, args.run_id))
        print(json.dumps(result, indent=2, default=str), flush=True)
        if result["status"] != "success":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
