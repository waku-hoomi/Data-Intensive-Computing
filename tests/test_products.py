from datetime import datetime, timezone
from types import SimpleNamespace
import json
from pyspark.sql import functions as F
from urban_platform.analytics import products
from urban_platform.analytics.context import register_analysis_views, sql_text
from urban_platform.analytics.verification import assert_same_results


def test_delta_products_metadata_refresh_and_query_equivalence(spark,tmp_path,monkeypatch):
    # Synthetic weather is explicitly defined in UTC for this test only.
    cfg={"timezone":"America/New_York","start_date":"2024-01-01",
         "end_date_exclusive":"2024-01-02","min_weather_hours":1,"schema_version":1}
    monkeypatch.setattr(products,"ROOT",tmp_path)
    monkeypatch.setattr(products,"analytics_config",lambda:{"analysis":cfg})
    monkeypatch.setattr(products,"require_weather_confirmation",lambda:None)
    rows=[("a",datetime(2024,1,1,5,tzinfo=timezone.utc),datetime(2024,1,1,6,tzinfo=timezone.utc),
           1,"Zone A","Brooklyn",2.,12.,1,10.,1,2024,1),
          ("b",datetime(2024,1,1,6,tzinfo=timezone.utc),datetime(2024,1,1,7,tzinfo=timezone.utc),
           1,"Zone A","Brooklyn",3.,15.,2,20.,1,2024,1)]
    df=spark.createDataFrame(rows,"trip_id string,pickup_ts timestamp,dropoff_ts timestamp,pu_location_id int,pickup_zone string,pickup_borough string,trip_distance double,fare_amount double,weather_condition_code int,pm25_ug_m3 double,aq_station_count long,pickup_year int,pickup_month int")
    source=tmp_path/"gold/integrated_taxi_trips"
    df.write.format("delta").save(str(source))
    records=products.build_products(spark,SimpleNamespace(gold_dir=str(tmp_path/"gold")))
    assert len(records)==4
    for record in records:
        assert record["source_delta_version"]==0
        assert record["active_files"]>0 and record["storage_bytes"]>0
        assert json.loads(record["analysis_config_json"])["timezone"]=="America/New_York"
        table=spark.read.format("delta").load(str(products.product_path(record["product_name"])))
        assert table.agg(F.sum("trip_count")).first()[0]==2
        table.createOrReplaceTempView("product_"+record["product_name"])
    register_analysis_views(spark,df,cfg)
    for query in ["01_zone_monthly","03_air_quality_demand","04_weather_variation","06_monthly_trend"]:
        assert_same_results(spark.sql(sql_text(query)).collect(),spark.sql(sql_text(query,"optimized")).collect())
    again=products.build_products(spark,SimpleNamespace(gold_dir=str(tmp_path/"gold")),["daily_mobility_summary"])[0]
    assert again["created_at_utc"]==records[0]["created_at_utc"]
    assert again["refreshed_at_utc"]>records[0]["refreshed_at_utc"]
    assert again["row_count"]==records[0]["row_count"]
    assert spark.read.format("delta").load(str(tmp_path/"data/lab2_metadata/product_registry")).count()==5
