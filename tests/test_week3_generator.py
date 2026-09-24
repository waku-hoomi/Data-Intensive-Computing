"""Source-level checks for the deterministic Week 3 release fixture."""

from __future__ import annotations

import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from urban_platform.simulation.generate import generate_release  # noqa: E402


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "raw"
    source.mkdir()
    trips = []
    for index in range(200):
        stamp = datetime(2024, 3, 1) + timedelta(minutes=index)
        trips.append({
            "VendorID": 1, "tpep_pickup_datetime": stamp,
            "tpep_dropoff_datetime": stamp + timedelta(minutes=13),
            "passenger_count": 1, "trip_distance": 2.0 + index / 100,
            "PULocationID": 132, "DOLocationID": 161,
            "fare_amount": 14.5, "total_amount": 18.0,
        })
    trips[0]["PULocationID"] = 264
    pq.write_table(pa.Table.from_pylist(trips), source / "yellow_tripdata_2024-01.parquet")
    _write_csv(source / "taxi_zone_lookup.csv",
               ["LocationID", "Borough", "Zone", "service_zone"],
               [{"LocationID": str(location_id), "Borough": "Queens",
                 "Zone": f"Zone {location_id}", "service_zone": "Boro Zone"}
                for location_id in (132, 161, 230)] +
               [{"LocationID": "264", "Borough": "N/A", "Zone": "N/A",
                 "service_zone": "N/A"}])
    weather_fields = ["year", "month", "day", "hour", "temp", "temp_source",
                      "rhum", "rhum_source", "prcp", "prcp_source", "coco"]
    weather = []
    for index in range(24 * 64):
        stamp = datetime(2024, 2, 1) + timedelta(hours=index)
        weather.append({"year": str(stamp.year), "month": str(stamp.month),
                        "day": str(stamp.day), "hour": str(stamp.hour),
                        "temp": "11.2", "temp_source": "original",
                        "rhum": "64", "rhum_source": "original",
                        "prcp": "0", "prcp_source": "original", "coco": "2"})
    _write_csv(source / "weather.csv", weather_fields, weather)
    aq_fields = ["State Code", "County Code", "Site Num", "Parameter Code", "POC",
                 "Latitude", "Longitude", "Parameter Name", "Date Local", "Time Local",
                 "Date GMT", "Time GMT", "Sample Measurement", "Units of Measure",
                 "State Name", "County Name", "Date of Last Change"]
    aq_rows = []
    for county in ("005", "047", "061", "081", "085"):
        for stamp in (datetime(2024, 2, 3, 12), datetime(2024, 4, 4, 12)):
            aq_rows.append({
                "State Code": "36", "County Code": county, "Site Num": "0001",
                "Parameter Code": "88101", "POC": "1", "Latitude": "40.75",
                "Longitude": "-73.90", "Parameter Name": "PM2.5 - Local Conditions",
                "Date Local": stamp.strftime("%Y-%m-%d"),
                "Time Local": stamp.strftime("%H:%M"),
                "Date GMT": stamp.strftime("%Y-%m-%d"),
                "Time GMT": stamp.strftime("%H:%M"),
                "Sample Measurement": "12.4",
                "Units of Measure": "Micrograms/cubic meter (LC)",
                "State Name": "New York", "County Name": "NYC",
                "Date of Last Change": "2024-04-05",
            })
    _write_csv(source / "air_quality" / "hourly_88101_2024.csv", aq_fields, aq_rows)
    return source


def test_generate_release_exact_counts_schema_timestamps_and_reproducibility(tmp_path):
    source = _source(tmp_path)
    output = tmp_path / "updates"
    manifest = generate_release(source, output, seed=77, hours=3)
    datasets = manifest["datasets"]
    assert datasets["taxi_trips"]["new_records"] == 15
    assert datasets["taxi_trips"]["duplicate_records"] == 3
    assert datasets["taxi_trips"]["eligible_source_rows"] == 199
    assert datasets["weather"]["new_records"] == 3
    assert datasets["weather"]["modified_records"] == 6
    assert datasets["air_quality"]["new_records"] == 15
    assert datasets["air_quality"]["modified_records"] == 5
    assert datasets["taxi_zone_lookup"]["modified_records"] == 3

    original = pq.read_table(source / "yellow_tripdata_2024-01.parquet")
    updated = pq.read_table(output / "taxi_trips.parquet")
    assert updated.schema.equals(original.schema)
    assert updated.num_rows == 18
    latest = max(max(original["tpep_pickup_datetime"].to_pylist()),
                 max(original["tpep_dropoff_datetime"].to_pylist()))
    before = [row for row in updated.to_pylist() if row["tpep_pickup_datetime"] <= latest]
    after = [row for row in updated.to_pylist() if row["tpep_pickup_datetime"] > latest]
    assert len(before) == 3 and len(after) == 15
    assert all(row in original.to_pylist() for row in before)
    assert all(row["tpep_dropoff_datetime"] > latest for row in after)

    weather = _rows(output / "weather.csv")
    assert len(weather) == 9 and "humidity" in weather[0]
    assert all(20 <= int(row["humidity"]) <= 100 for row in weather)
    aq = _rows(output / "air_quality.csv")
    assert len(aq) == 20 and "aqi" in aq[0]
    assert all(0 <= int(row["aqi"]) <= 500 for row in aq)
    assert len(_rows(output / "taxi_zone_lookup.csv")) == 3

    hashes = {name: record["sha256"] for name, record in datasets.items()}
    repeated = generate_release(source, output, seed=77, hours=3)
    assert hashes == {name: record["sha256"] for name, record in repeated["datasets"].items()}
