"""Validate all final product totals and export reproducible source/runtime evidence."""
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from urban_platform.analytics.context import analytics_config,register_analysis_views,read_integrated
from urban_platform.analytics.products import PRODUCTS,product_path
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark


def main():
    spark=get_spark("lab2-final-evidence")
    try:
        cfg=load_config(str(ROOT/"config/datasets.yaml"))
        register_analysis_views(spark,read_integrated(spark,cfg))
        scoped=spark.table("analysis_trips")
        scope=scoped.agg(F.count("*").alias("rows"),
            F.sum(F.col("pu_location_id").isNotNull().cast("long")).alias("rows_with_zone_id"),
            F.sum(F.col("pickup_borough").isNotNull().cast("long")).alias("rows_with_borough"),
            F.sum(F.col("weather_condition_code").isNull().cast("long")).alias("missing_weather_code"),
            F.sum(F.col("pm25_ug_m3").isNull().cast("long")).alias("missing_air_quality")).first().asDict()
        summary={"scope":scope,"elapsed_hours":spark.table("hour_spine").count(),"products":[]}
        for name in PRODUCTS:
            path=str(product_path(name))
            df=spark.read.format("delta").load(path)
            total=int(df.agg(F.sum("trip_count")).first()[0])
            expected=(scope["rows_with_borough"] if name=="air_quality_impact_summary" else
                      scope["rows_with_zone_id"] if name=="weather_impact_summary" else scope["rows"])
            assert total==expected,(name,total,expected)
            summary["products"].append({"name":name,"trip_count_total":total,"expected_total":expected,"passed":True})
        env={"python":platform.python_version(),"spark":spark.version,
             "delta_spark":importlib.metadata.version("delta-spark"),"os":platform.platform(),
             "java":spark._jvm.java.lang.System.getProperty("java.version"),
             "master":spark.sparkContext.master,"driver_memory":spark.sparkContext.getConf().get("spark.driver.memory"),
             "shuffle_partitions":spark.conf.get("spark.sql.shuffle.partitions"),
             "session_timezone":spark.conf.get("spark.sql.session.timeZone"),"analysis_config":analytics_config()["analysis"]}
        env.update(cpu=platform.processor() or platform.machine(),logical_cpus=os.cpu_count(),memory_bytes=None)
        try:
            env["memory_bytes"]=os.sysconf("SC_PHYS_PAGES")*os.sysconf("SC_PAGE_SIZE")
        except (ValueError,OSError):
            pass
        if sys.platform=="darwin":
            for key,name in [("machdep.cpu.brand_string","cpu"),("hw.memsize","memory_bytes"),("hw.ncpu","logical_cpus")]:
                env[name]=subprocess.check_output(["/usr/sbin/sysctl","-n",key],text=True).strip()
        gold=str(Path(cfg.gold_dir)/"integrated_taxi_trips")
        detail=DeltaTable.forPath(spark,gold).detail().first()
        summary["gold_storage"]={"active_bytes":int(detail.sizeInBytes),"active_files":int(detail.numFiles),
                                 "delta_version":int(DeltaTable.forPath(spark,gold).history(1).first().version)}
        units=spark.read.format("delta").load(str(Path(cfg.bronze_dir)/"air_quality")).select("units").distinct().collect()
        summary["air_quality_units"]=[r.units for r in units]
        folder=ROOT/"artifacts"
        (folder/"final_validation.json").write_text(json.dumps(summary,indent=2,default=str))
        (folder/"benchmark_environment.json").write_text(json.dumps(env,indent=2,default=str))
        print(json.dumps(summary,indent=2),flush=True)
    finally:
        spark.stop()


if __name__=="__main__":
    main()
