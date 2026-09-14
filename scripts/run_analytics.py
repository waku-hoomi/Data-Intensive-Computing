"""Execute all six Spark SQL queries and retain JSON results and physical plans."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from urban_platform.analytics.context import QUERY_IDS, WEATHER_QUERIES, require_weather_confirmation, register_analysis_views, read_integrated, sql_text
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--query",choices=QUERY_IDS+["all"],default="all")
    args=parser.parse_args()
    queries=QUERY_IDS if args.query=="all" else [args.query]
    if WEATHER_QUERIES.intersection(queries):
        require_weather_confirmation()
    spark=get_spark("lab2-analytical-queries")
    try:
        cfg=load_config(str(ROOT/"config/datasets.yaml"))
        register_analysis_views(spark,read_integrated(spark,cfg))
        output=ROOT/"artifacts/query_results"
        output.mkdir(parents=True,exist_ok=True)
        for query in queries:
            sql=sql_text(query)
            rows=spark.sql(sql).collect()
            (output/f"{query}.json").write_text(json.dumps([r.asDict() for r in rows],indent=2,default=str))
            (output/f"{query}.plan.txt").write_text("\n".join(r[0] for r in spark.sql("EXPLAIN FORMATTED "+sql).collect()))
            print(f"{query}: {len(rows)} result rows",flush=True)
    finally:
        spark.stop()
