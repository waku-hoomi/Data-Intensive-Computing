"""Controlled query experiments, retaining every timing, result and physical plan.

These are warm-process local benchmarks. OS file caches are not flushed.
Explicit Spark caches are cleared between variants. Setup/materialization costs
are measured separately. Pair order alternates to reduce fixed-order bias.
"""
import datetime as dt
import json
import statistics
import time
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from urban_platform.analytics.context import ROOT, QUERY_IDS, analytics_config, sql_text
from urban_platform.analytics.products import PRODUCTS, product_path
from urban_platform.analytics.verification import assert_same_results
from urban_platform.monitoring.publication import latest_publication


def save_json(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2,default=str,allow_nan=False))


def configure_variant(spark, aqe=False, cache=None):
    spark.catalog.clearCache()
    spark.conf.set("spark.sql.adaptive.enabled",str(aqe).lower())
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions",str(analytics_config()["benchmark"]["shuffle_partitions"]))
    start=time.perf_counter()
    if cache:
        spark.sql(f"CACHE TABLE {cache}")
        spark.table(cache).count()
    return time.perf_counter()-start


def run_pair(spark, name, before, after, before_options=None, after_options=None):
    cfg=analytics_config()["benchmark"]
    output=ROOT/"artifacts/benchmarks"/name
    output.mkdir(parents=True,exist_ok=True)
    variants={"before":(before,before_options or {}),"after":(after,after_options or {})}
    collected={}
    schemas={}
    records={"before":[],"after":[]}
    # Each variant is warmed immediately before its own measured execution;
    # cache population and warm-up are never counted as query latency.
    for repetition in range(cfg["measured_runs"]):
        order=["before","after"] if repetition%2==0 else ["after","before"]
        for label in order:
            sql,options=variants[label]
            setup_seconds=configure_variant(spark,**options)
            for _ in range(cfg["warmup_runs"]):
                spark.sql(sql).collect()
            df=spark.sql(sql)
            start=time.perf_counter()
            rows=df.collect()
            seconds=time.perf_counter()-start
            if label in collected:
                assert_same_results(collected[label],rows,cfg["relative_tolerance"],cfg["absolute_tolerance"])
            else:
                collected[label]=rows
                schemas[label]=df.schema.simpleString()
            if repetition==0:
                formatted="\n".join(r[0] for r in spark.sql("EXPLAIN FORMATTED "+sql).collect())
                (output/f"{label}.explain_formatted.txt").write_text(formatted)
                (output/f"{label}.executed_plan.txt").write_text(df._jdf.queryExecution().executedPlan().toString())
                (output/f"{label}.sql").write_text(sql)
                # JSON is a review artifact; undefined correlation is serialized
                # as null, while comparison uses the original Spark values.
                import math
                values=[{k:(None if isinstance(v,float) and not math.isfinite(v) else v)
                         for k,v in r.asDict().items()} for r in rows]
                save_json(output/f"{label}.results.json",values)
            records[label].append({"repetition":repetition+1,"execution_seconds":seconds,
                                   "setup_seconds":setup_seconds,"order_in_pair":order.index(label)+1})
            print(f"{name} {label} run {repetition+1}: {seconds:.3f}s",flush=True)
    assert_same_results(collected["before"],collected["after"],cfg["relative_tolerance"],cfg["absolute_tolerance"])
    if schemas["before"]!=schemas["after"]:
        raise AssertionError(f"Result schemas differ: {schemas}")
    medians={label:statistics.median(r["execution_seconds"] for r in rows) for label,rows in records.items()}
    result={"name":name,"records":records,"median_seconds":medians,
            "speedup":medians["before"]/medians["after"],"results_equal":True,
            "result_rows":len(collected["before"]),"schema":schemas["before"],
            "before_options":before_options or {},"after_options":after_options or {},
            "config":cfg,"created_at_utc":dt.datetime.now(dt.timezone.utc).isoformat()}
    save_json(output/"measurements.json",result)
    spark.catalog.clearCache()
    return result


def run_experiments(spark, platform_cfg, suite="all"):
    records=[]
    source_path=str(Path(platform_cfg.gold_dir)/"integrated_taxi_trips")
    publication=latest_publication(spark,platform_cfg)
    source_version=(publication["gold_version"] if publication else
                    int(DeltaTable.forPath(spark,source_path).history(1).first().version))
    effective_cfg=publication["analysis_config"] if publication else analytics_config()["analysis"]
    manifest={"source_delta_version":source_version,"analysis_config":effective_cfg,
              "spark":spark.version,"master":spark.sparkContext.master,
              "measurement_scope":"Warm-process local execution; OS caches not flushed"}
    save_json(ROOT/"artifacts"/f"benchmark_{suite}_manifest.json",manifest)
    if suite in ("all","techniques"):
        query=sql_text("02_weather_distance")
        records.append(run_pair(spark,"technique_cache",query,query,after_options={"cache":"analysis_trips"}))
        base=sql_text("01_zone_monthly").replace("FROM analysis_trips",
                    "FROM analysis_trips WHERE local_month='2024-02'")
        optimized=base.replace("WHERE local_month='2024-02'",
                    "WHERE local_month='2024-02' AND pickup_year=2024 AND pickup_month IN (2,3)")
        records.append(run_pair(spark,"technique_partition_pruning",base,optimized))
        for name in ("taxi_trips","taxi_zone_lookup"):
            spark.read.format("delta").load(str(Path(platform_cfg.bronze_dir)/name)).createOrReplaceTempView("bronze_"+name)
        join="""SELECT {hint} z.borough,count(*) AS trip_count
          FROM bronze_taxi_trips t LEFT JOIN bronze_taxi_zone_lookup z ON t.pu_location_id=z.location_id
          WHERE t.pickup_ts >= TIMESTAMP '2024-01-01 05:00:00'
            AND t.pickup_ts < TIMESTAMP '2024-04-01 04:00:00'
          GROUP BY z.borough"""
        records.append(run_pair(spark,"technique_broadcast",join.format(hint="/*+ MERGE(t,z) */"),
                                join.format(hint="/*+ BROADCAST(z) */")))
        records.append(run_pair(spark,"technique_aqe",sql_text("01_zone_monthly"),sql_text("01_zone_monthly"),
                                after_options={"aqe":True}))
    if suite in ("all","queries"):
        registry=spark.read.format("delta").load(str(ROOT/"data/lab2_metadata/product_registry"))
        for name in PRODUCTS:
            if publication:
                product_version=publication["product_versions"][name]
                product=(spark.read.format("delta").option("versionAsOf",product_version)
                         .load(str(product_path(name))))
            else:
                latest=(registry.filter(F.col("product_name")==name)
                        .orderBy(F.desc("refreshed_at_utc")).first())
                if latest is None or latest.source_delta_version!=source_version:
                    raise ValueError(f"Rebuild stale or missing product before benchmarking: {name}")
                if json.loads(latest.analysis_config_json)!=effective_cfg:
                    raise ValueError(f"Product uses a different analysis configuration: {name}")
                product=spark.read.format("delta").load(str(product_path(name)))
            product.createOrReplaceTempView("product_"+name)
        for query in QUERY_IDS:
            if query=="02_weather_distance":
                after=sql_text(query)
                options={"cache":"analysis_trips"}
            elif query=="05_peak_hours":
                after=sql_text(query)
                options={"cache":"city_hours"}
            else:
                after=sql_text(query,"optimized")
                options={}
            records.append(run_pair(spark,"query_"+query,sql_text(query),after,after_options=options))
    save_json(ROOT/"artifacts"/f"benchmark_{suite}.json",records)
    return records
