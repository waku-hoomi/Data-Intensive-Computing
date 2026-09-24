"""Fault injection against real Bronze, Gold, product and publication Delta tables."""
import csv
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from urban_platform.analytics import products, refresh
from urban_platform.analytics.context import register_analysis_views, sql_text
from urban_platform.analytics.verification import assert_same_results
from urban_platform.incremental import runner
from urban_platform.monitoring.publication import latest_publication
from urban_platform.utils.config import load_config

ROOT = Path(__file__).resolve().parents[1]


def _csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows({c: row.get(c, '') for c in fields} for row in rows)


def _fixture(tmp_path, monkeypatch):
    cfg = load_config(str(ROOT / 'config/datasets.yaml'))
    cfg = replace(cfg, raw_data_dir=str(tmp_path / 'raw'),
                  bronze_dir=str(tmp_path / 'data/bronze'), gold_dir=str(tmp_path / 'data/gold'),
                  metadata_dir=str(tmp_path / 'data/_metadata'))
    analysis = {'timezone':'America/New_York', 'start_date':'2024-01-01',
                'end_date_exclusive':'2024-01-02', 'min_weather_hours':1,
                'schema_version':3, 'weather_interpretation_approved':True}
    monkeypatch.setattr(products, 'ROOT', tmp_path)
    for module in (products, refresh, runner):
        monkeypatch.setattr(module, 'analytics_config', lambda: {'analysis': analysis})
    monkeypatch.setattr(products, 'require_weather_confirmation', lambda: None)
    raw = Path(cfg.raw_data_dir)
    raw.mkdir()
    zones = [{'LocationID':i, 'Borough':'Brooklyn', 'Zone':f'Zone {i}', 'service_zone':'Boro Zone'} for i in (1,2)]
    weather = [{'year':2024,'month':1,'day':1,'hour':h,'temp':10,'rhum':60,'coco':1} for h in (6,7)]
    aq = [{'State Code':'36','County Code':'047','Site Num':'0001','Parameter Code':'88101',
           'POC':1,'Date GMT':'2024-01-01','Time GMT':f'{h:02}:00','Sample Measurement':10,
           'Parameter Name':'PM2.5 - Local Conditions','County Name':'Kings'} for h in (6,7)]
    for name, rows in [('taxi_zone_lookup',zones),('weather',weather),('air_quality',aq)]:
        ds = cfg.get_dataset(name)
        _csv(raw / ds.input_path, ds.expected_raw_columns, rows)
    trips = []
    for hour in (1,2):
        row = {c:1 for c in cfg.get_dataset('taxi_trips').expected_raw_columns}
        row.update(tpep_pickup_datetime=datetime(2024,1,1,hour),
                   tpep_dropoff_datetime=datetime(2024,1,1,hour,15),
                   PULocationID=hour,DOLocationID=1,trip_distance=2.,fare_amount=10.,
                   store_and_fwd_flag='N')
        trips.append(row)
    taxi = pa.Table.from_pylist(trips)
    pq.write_table(taxi, raw / 'yellow_tripdata_2024-01.parquet')
    update = tmp_path / 'updates'
    update.mkdir()
    pq.write_table(taxi.slice(0,0), update / 'taxi_trips.parquet')
    for name in ('taxi_zone_lookup','weather','air_quality'):
        _csv(update / runner.UPDATE_FILES[name], cfg.get_dataset(name).expected_raw_columns, [])
    return cfg, update, zones, weather, analysis


def _check_products(spark, cfg, analysis):
    gold = spark.read.format('delta').load(str(Path(cfg.gold_dir) / 'integrated_taxi_trips'))
    register_analysis_views(spark, gold, analysis)
    for name in products.PRODUCTS:
        actual = spark.read.format('delta').load(str(products.product_path(name))).collect()
        expected = spark.sql(sql_text(name, 'products')).collect()
        assert_same_results(actual, expected)


def test_failure_after_gold_and_partial_product_with_new_batch(spark, tmp_path, monkeypatch):
    cfg, update, zones, weather, analysis = _fixture(tmp_path, monkeypatch)
    baseline = runner.bootstrap_platform(spark, cfg, 'recovery-baseline')
    assert baseline['status'] == 'success', baseline
    original_publication = latest_publication(spark, cfg)
    fields = cfg.get_dataset('taxi_zone_lookup').expected_raw_columns
    zones[0]['Zone'] = 'Zone 1 corrected'
    _csv(update / 'taxi_zone_lookup.csv', fields, [zones[0]])

    real_refresh = runner.refresh_affected_products
    def fail_after_gold(*args, **kwargs):
        raise RuntimeError('injected after Gold commit')
    monkeypatch.setattr(runner, 'refresh_affected_products', fail_after_gold)
    failed = runner.process_updates(spark, cfg, update, 'after-gold-failure')
    assert failed['status'] == 'failed'
    assert failed['gold']['after_version'] > original_publication['gold_version']
    assert latest_publication(spark, cfg)['run_id'] == original_publication['run_id']
    # The old publication still points to products with the original zone name.
    monkeypatch.setattr(runner, 'refresh_affected_products', real_refresh)
    retry = runner.process_updates(spark, cfg, update, 'after-gold-retry')
    assert retry['status'] == 'success', retry
    assert retry['products']['taxi_zone_statistics']['mode'] == 'groups'
    _check_products(spark, cfg, analysis)

    published = latest_publication(spark, cfg)
    zones[0]['Zone'] = 'intermediate label'
    _csv(update / 'taxi_zone_lookup.csv', fields, [zones[0]])
    real_record = refresh.record_product_refresh
    def fail_after_product(*args, **kwargs):
        real_record(*args, **kwargs)
        raise RuntimeError('injected after product commit')
    monkeypatch.setattr(refresh, 'record_product_refresh', fail_after_product)
    partial = runner.process_updates(spark, cfg, update, 'partial-product-failure')
    assert partial['status'] == 'failed'
    assert latest_publication(spark, cfg)['run_id'] == published['run_id']
    # A new key arrives while zone 1 is still unpublished: both must reach Gold.
    zones[1]['Zone'] = 'Zone 2 corrected'
    _csv(update / 'taxi_zone_lookup.csv', fields, [zones[1]])
    monkeypatch.setattr(refresh, 'record_product_refresh', real_record)
    recovered = runner.process_updates(spark, cfg, update, 'mixed-batch-retry')
    assert recovered['status'] == 'success', recovered
    assert recovered['bronze']['taxi_zone_lookup']['unpublished_change_keys'] == 2
    gold = spark.read.format('delta').load(str(Path(cfg.gold_dir) / 'integrated_taxi_trips'))
    assert {r.pickup_zone for r in gold.collect()} == {'intermediate label','Zone 2 corrected'}
    _check_products(spark, cfg, analysis)
    # A completed replay is idempotent and does not refresh products.
    again = runner.process_updates(spark, cfg, update, 'completed-replay')
    assert again['status'] == 'success', again
    assert all(m['inserted'] == m['updated'] == 0 for m in again['bronze'].values())
    assert all(m['mode'] == 'skipped' for m in again['products'].values())


def test_multicommit_revert_keeps_intermediate_product_grains(spark, tmp_path):
    # Real CDF: 1 -> 3 -> 1. A partially committed weather product could retain 3.
    from test_week3_refresh import _row, SCHEMA
    from urban_platform.analytics.refresh import _paired_changes, _changed_grains
    path = str(tmp_path / 'gold')
    initial = spark.createDataFrame([_row('insert', condition=1)], SCHEMA).drop('_change_type')
    initial.write.format('delta').option('delta.enableChangeDataFeed','true').save(path)
    table = DeltaTable.forPath(spark, path)
    table.update(set={'weather_condition_code': F.lit(3)})
    table.update(set={'weather_condition_code': F.lit(1)})
    cdf = spark.read.format('delta').option('readChangeFeed','true').option('startingVersion',1).load(path)
    assert _paired_changes(cdf).count() == 2  # not the four-row cross-commit join
    assert refresh.changed_product_names(cdf) == {'weather_impact_summary'}
    grains = _changed_grains(cdf, 'weather_impact_summary', 'America/New_York').collect()
    assert {r.weather_condition_code for r in grains} == {1,3}
