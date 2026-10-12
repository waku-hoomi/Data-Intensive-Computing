#!/usr/bin/env python3
import argparse
import platform
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.common import ROOT, load_config, save_json, spark_session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["ingest", "prepare", "discover", "evaluate"])
    parser.add_argument("--storage", default=str(ROOT / "data/processed"))
    parser.add_argument("--source")
    parser.add_argument("--config")
    args = parser.parse_args()
    config = load_config(args.config)
    start = time.perf_counter()
    if args.stage == "ingest":
        from retail.ingest import ingest
        ingest(args.source)
    else:
        spark = spark_session("retail-" + args.stage, config["shuffle_partitions"])
        try:
            if args.stage == "prepare":
                from retail.clean import prepare
                prepare(spark, config, args.storage)
            elif args.stage == "discover":
                from retail.mining import discover
                discover(spark, config, args.storage)
            else:
                from retail.mining import evaluate
                evaluate(spark, config, args.storage)
        finally:
            spark.stop()
    save_json(ROOT / f"results/run_{args.stage}.json", {"stage": args.stage, "seconds": time.perf_counter() - start,
              "storage": args.storage, "python": platform.python_version(), "platform": platform.platform()})
    print(f"Completed {args.stage}", flush=True)


if __name__ == "__main__":
    main()
