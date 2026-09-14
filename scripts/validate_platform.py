"""Record structural, geographic and candidate-key checks on actual Delta inputs."""
import argparse
import json
import sys
from pathlib import Path
from pyspark.sql import functions as F
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark
from urban_platform.integration.integrate import aggregate_nyc_air_quality


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--bronze-only",action="store_true")
    args=parser.parse_args()
    spark=get_spark("lab2-data-validation")
    cfg=load_config(str(ROOT/"config/datasets.yaml"))
    result={}
    try:
        def read(name):
            return spark.read.format("delta").load(str(Path(cfg.bronze_dir)/name))
        trips=read("taxi_trips")
        stats=trips.agg(F.count("*").alias("rows"),F.countDistinct("trip_id").alias("unique_trip_ids"),
                        F.sum(F.when(F.col("dropoff_ts")<F.col("pickup_ts"),1).otherwise(0)).alias("negative_durations")).first().asDict()
        assert stats["rows"]==stats["unique_trip_ids"] and stats["negative_durations"]==0
        result["taxi_bronze"]=stats
        candidate=["vendor_id","pickup_ts","dropoff_ts","pu_location_id","do_location_id"]
        result["candidate_composite_key"]={"columns":candidate,
            "duplicate_groups":trips.groupBy(*candidate).count().filter("count > 1").count(),
            "note":"Profiles cleaned data; does not prove the same key is valid in future deliveries."}
        zones=read("taxi_zone_lookup")
        assert zones.count()==zones.select("location_id").distinct().count()
        aq=read("air_quality")
        result["air_quality_county_states"]=[r.asDict() for r in aq.filter(F.col("county_name").isin("Kings","Bronx","Queens"))
                                              .groupBy("state_name","county_name").count().collect()]
        # Recalculate NYC aggregates after removing every out-of-state row:
        # outputs must be identical, including the station count.
        a=aggregate_nyc_air_quality(aq)
        b=aggregate_nyc_air_quality(aq.filter(F.col("station_id").startswith("36-")))
        assert a.exceptAll(b).limit(1).count()==0 and b.exceptAll(a).limit(1).count()==0
        result["nyc_air_quality_geographic_invariance"]=True
        result["aq_hourly_rows"]=a.count()
        if not args.bronze_only:
            weather=read("weather")
            assert weather.count()==weather.select("observation_ts").distinct().count()
            gold=spark.read.format("delta").load(str(Path(cfg.gold_dir)/"integrated_taxi_trips"))
            result["integrated"]=gold.agg(F.count("*").alias("rows"),F.countDistinct("trip_id").alias("unique_trip_ids"),
                F.sum(F.col("weather_condition_code").isNull().cast("long")).alias("missing_weather_code"),
                F.sum(F.col("temp_c").isNull().cast("long")).alias("missing_temperature"),
                F.sum(F.col("pm25_ug_m3").isNull().cast("long")).alias("missing_air_quality")).first().asDict()
            assert result["integrated"]["rows"]==stats["rows"]==result["integrated"]["unique_trip_ids"]
            assert gold.select("trip_id").exceptAll(trips.select("trip_id")).limit(1).count()==0
            assert (gold.withColumn("pickup_hour",F.date_trunc("hour","pickup_ts"))
                    .groupBy("pickup_hour").agg(F.countDistinct("weather_condition_code").alias("n"))
                    .filter("n > 1").count()==0)
        path=ROOT/"artifacts"/("validation_bronze.json" if args.bronze_only else "validation_platform.json")
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(result,indent=2,default=str))
        print(json.dumps(result,indent=2,default=str),flush=True)
    finally:
        spark.stop()


if __name__=="__main__":
    main()
