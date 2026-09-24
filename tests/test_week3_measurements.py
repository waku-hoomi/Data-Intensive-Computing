"""Check that overhead experiments account for data and consume all columns."""
import csv
from dataclasses import replace
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from urban_platform.benchmark.production import storage_inventory, validation_pair, fingerprint
from urban_platform.utils.config import load_config


def test_storage_accounts_for_active_history_cdf_logs_and_operational_files(spark, tmp_path):
    table = tmp_path / 'data/bronze/example'
    spark.range(3).withColumn('value', F.lit(1)).write.format('delta').option(
        'delta.enableChangeDataFeed','true').save(str(table))
    before = storage_inventory(spark, tmp_path)
    DeltaTable.forPath(spark, str(table)).update(set={'value': F.lit(2)})
    for relative in ('_metadata/quarantine/rejected.bin', '_metadata/classified/stage.bin',
                     '_metadata/change_keys/keys.bin', 'monitoring/extra.bin'):
        p = tmp_path / 'data' / relative
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b'0123456789')
    after = storage_inventory(spark, tmp_path)
    assert after['total_logical_bytes'] == sum(p.stat().st_size for p in (tmp_path/'data').rglob('*') if p.is_file())
    bronze = after['groups']['bronze']
    assert bronze['active_data_bytes'] > 0
    assert bronze['retained_data_bytes'] > 0
    assert bronze['cdf_bytes'] > 0 and bronze['delta_log_bytes'] > 0
    assert after['operational_and_retained_bytes'] > before['operational_and_retained_bytes']
    assert after['groups']['metadata/quarantine']['total_bytes'] == 10
    assert after['groups']['metadata/classified']['total_bytes'] == 10
    assert after['groups']['metadata/change_keys']['total_bytes'] == 10
    assert after['total_logical_bytes'] == after['active_business_data_bytes'] + after['operational_and_retained_bytes']


def test_validation_controls_consume_all_values_and_preserve_rejected_rows(spark, tmp_path):
    cfg = load_config(str(Path(__file__).resolve().parents[1] / 'config/datasets.yaml'))
    cfg = replace(cfg, metadata_dir=str(tmp_path/'data/_metadata'))
    weather = cfg.get_dataset('weather')
    columns = weather.expected_raw_columns + ['humidity']
    rows = [{'year':2024,'month':1,'day':1,'hour':h,'temp':t,'humidity':v}
            for h,t,v in [(0,10,50),(1,20,200),(2,30,'bad')]]
    with (tmp_path/'weather.csv').open('w',newline='') as stream:
        writer = csv.DictWriter(stream,fieldnames=columns)
        writer.writeheader()
        writer.writerows({c:r.get(c,'') for c in columns} for r in rows)
    result = validation_pair(spark,cfg,tmp_path,{'weather':'weather.csv'},enabled_first=False)
    measured = result['datasets']['weather']
    assert measured['rows'] == 3 and measured['invalid_or_duplicate_rows'] == 2
    assert measured['identical_input_and_sink']
    # Non-key payload changes must change the sink, even when row counts match.
    a = spark.range(3).withColumn('otherwise_prunable_payload',F.lit('a'))
    b = a.withColumn('otherwise_prunable_payload',F.lit('b'))
    assert fingerprint(a) != fingerprint(b)
