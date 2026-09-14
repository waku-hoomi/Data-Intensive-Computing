"""Run selected validated inputs; weather requires an explicit provenance decision."""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from urban_platform.analytics.context import require_weather_confirmation
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark
from urban_platform.ingestion.pipeline import ingest_dataset

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("datasets",nargs="+",choices=["taxi_zone_lookup","taxi_trips","air_quality","weather"])
    args=parser.parse_args()
    if "weather" in args.datasets:
        require_weather_confirmation()
    cfg=load_config(str(ROOT/"config/datasets.yaml"))
    spark=get_spark("lab2-validated-ingestion")
    try:
        for name in args.datasets:
            print(ingest_dataset(spark,cfg,name),flush=True)
    finally:
        spark.stop()
