"""Small Delta integration checks for Week 3 validation and Bronze MERGE."""
import csv
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from urban_platform.ingestion.pipeline import (
    SchemaValidationError,
    _save_raw_schema,
    _validate_raw_schema,
    apply_incremental_dataset,
    changed_keys_since_publication,
)
from urban_platform.ingestion.quality import validate_records
from urban_platform.utils.config import load_config


ROOT = Path(__file__).resolve().parents[1]


def _weather_csv(path, config, rows, *, humidity=False):
    columns = list(config.expected_raw_columns) + (["humidity"] if humidity else [])
    with open(path, "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({**{column: "" for column in columns}, **row})


def test_strict_raw_schema_rejects_unknown_and_incompatible_additions(spark):
    config = load_config(str(ROOT / "config/datasets.yaml")).get_dataset("weather")
    row = {column: "" for column in config.expected_raw_columns}
    ordinary = spark.createDataFrame([row])
    _validate_raw_schema(ordinary, config, strict=True)
    with pytest.raises(SchemaValidationError, match="unknown raw columns"):
        _validate_raw_schema(ordinary.withColumn("new_unapproved_column", ordinary.year), config, strict=True)
    with pytest.raises(SchemaValidationError, match="incompatible additive type"):
        _validate_raw_schema(ordinary.withColumn("humidity", ordinary.year.cast("int")), config, strict=True)


def test_missing_zone_and_incomplete_trip_are_quarantined(spark):
    config = load_config(str(ROOT / "config/datasets.yaml")).get_dataset("taxi_trips")
    config = replace(config, required_fields=["trip_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id"],
                     numeric_rules=[])
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
    trips = spark.createDataFrame([
        ("valid", timestamp, timestamp, 1, 2),
        ("unknown", timestamp, timestamp, 99, 2),
        ("incomplete", timestamp, timestamp, 1, None),
    ], "trip_id string,pickup_ts timestamp,dropoff_ts timestamp,pu_location_id int,do_location_id int")
    zones = spark.createDataFrame([(1,), (2,)], "location_id int")
    clean, rejected, report = validate_records(trips, config, {"taxi_zone_lookup": zones})
    assert [row.trip_id for row in clean.collect()] == ["valid"]
    reasons = {row.trip_id: row["_reject_reason"] for row in rejected.collect()}
    assert "missing_foreign_key:pu_location_id" in reasons["unknown"]
    assert "missing_required:do_location_id" in reasons["incomplete"]
    assert report.rejected_rows == 2


def test_weather_incremental_merge_schema_quarantine_and_idempotence(spark, tmp_path):
    config = load_config(str(ROOT / "config/datasets.yaml"))
    config = replace(config, bronze_dir=str(tmp_path / "bronze"),
                     metadata_dir=str(tmp_path / "metadata"))
    weather = config.get_dataset("weather")
    baseline_path = tmp_path / "baseline.csv"
    _weather_csv(baseline_path, weather, [
        {"year": "2024", "month": "1", "day": "1", "hour": "0", "temp": "10", "rhum": "50"},
    ])
    baseline_raw = spark.read.options(header="true", inferSchema="false").csv(str(baseline_path))
    _save_raw_schema(config, "weather", baseline_raw)
    (weather.resolve_transform_fn()(baseline_raw).write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .save(str(tmp_path / "bronze" / "weather")))

    update_path = tmp_path / "weather_update.csv"
    new_hour = {"year": "2024", "month": "1", "day": "1", "hour": "1",
                "temp": "12", "rhum": "55", "humidity": "56"}
    _weather_csv(update_path, weather, [
        {"year": "2024", "month": "1", "day": "1", "hour": "0",
         "temp": "11", "rhum": "50", "humidity": "51"},
        new_hour,
        new_hour,
        {"year": "2024", "month": "1", "day": "1", "hour": "2",
         "temp": "13", "rhum": "60", "humidity": "bad"},
    ], humidity=True)
    metrics, changed = apply_incremental_dataset(spark, config, "weather", str(update_path), "test_update")
    assert metrics["status"] == "success", metrics["validation_failures"]
    assert (metrics["inserted"], metrics["updated"], metrics["duplicates"], metrics["rejected"]) == (1, 1, 1, 1)
    assert changed.count() == 2
    merged = spark.read.format("delta").load(str(tmp_path / "bronze" / "weather"))
    assert "humidity_pct_v2" in merged.columns
    values = {
        row.hour: row.temp_c
        for row in merged.selectExpr(
            "date_format(observation_ts, 'HH') AS hour", "temp_c"
        ).collect()
    }
    assert values == {"00": 11.0, "01": 12.0}
    quarantined = spark.read.format("delta").load(metrics["quarantine_path"])
    assert {row["_reject_reason"] for row in quarantined.collect()} == {
        "duplicate_primary_key", "missing_required:humidity_pct_v2"}

    recovered = changed_keys_since_publication(spark, config, "weather", 0)
    assert recovered is not None and recovered.count() == 2

    repeated, changed_again = apply_incremental_dataset(spark, config, "weather", str(update_path), "repeat_update")
    assert repeated["status"] == "success", repeated["validation_failures"]
    assert (repeated["inserted"], repeated["updated"], repeated["duplicates"], repeated["rejected"]) == (0, 0, 3, 1)
    assert repeated["before_version"] == repeated["after_version"]
    assert changed_again.count() == 0
