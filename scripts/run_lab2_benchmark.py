import argparse
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from urban_platform.analytics.context import require_weather_confirmation,register_analysis_views,read_integrated
from urban_platform.analytics.experiments import run_experiments
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--suite",choices=["all","techniques","queries"],default="all")
    args=parser.parse_args()
    require_weather_confirmation()
    spark=get_spark("lab2-reproducible-benchmark")
    try:
        cfg=load_config(str(ROOT/"config/datasets.yaml"))
        register_analysis_views(spark,read_integrated(spark,cfg))
        run_experiments(spark,cfg,args.suite)
    finally:
        spark.stop()
