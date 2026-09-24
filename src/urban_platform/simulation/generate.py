"""Generate small, reproducible source releases without loading source tables in RAM.

The taxi source is scanned twice in Arrow batches. The first pass profiles its
original row count, latest timestamp and eligible rows; the second samples an
exact number of eligible rows and writes one Parquet file in bounded batches.
The much larger EPA CSV is scanned once, retaining only a few NYC templates.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


NYC_COUNTIES = ("005", "047", "061", "081", "085")
TAXI_COLUMNS = (
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
    "fare_amount", "total_amount",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _mask(batch: pa.RecordBatch, known_zones: set[int]) -> np.ndarray:
    """Select source rows usable as both realistic new trips and valid duplicates."""
    def col(name: str) -> pa.Array:
        return batch.column(batch.schema.get_field_index(name))

    pickup = col("tpep_pickup_datetime")
    dropoff = col("tpep_dropoff_datetime")
    valid = pc.and_(pc.greater_equal(pickup, pa.scalar(datetime(2000, 1, 1))),
                    pc.less(pickup, pa.scalar(datetime(2100, 1, 1))))
    valid = pc.and_(valid, pc.greater_equal(dropoff, pickup))
    valid = pc.and_(valid, pc.less_equal(pc.subtract(dropoff, pickup),
                                         pa.scalar(timedelta(hours=6))))
    valid = pc.and_(valid, pc.greater(col("VendorID"), 0))
    valid = pc.and_(valid, pc.is_in(col("PULocationID"),
                                   value_set=pa.array(sorted(known_zones), type=col("PULocationID").type)))
    valid = pc.and_(valid, pc.is_in(col("DOLocationID"),
                                   value_set=pa.array(sorted(known_zones), type=col("DOLocationID").type)))
    for name, minimum, maximum in (
        ("trip_distance", 0, 500), ("fare_amount", 0, 2000),
        ("total_amount", 0, 3000), ("passenger_count", 1, 9),
    ):
        value = col(name)
        valid = pc.and_(valid, pc.and_(pc.greater_equal(value, minimum),
                                        pc.less_equal(value, maximum)))
    return pc.fill_null(valid, False).to_numpy(zero_copy_only=False)


def _arrow_batches(files: list[Path], *, columns: tuple[str, ...] | None = None):
    for file in files:
        parquet = pq.ParquetFile(file)
        for batch in parquet.iter_batches(batch_size=131_072, columns=columns):
            yield batch


def _taxi_profile(files: list[Path], zones: set[int]) -> tuple[int, int, datetime, pa.Schema]:
    schemas = [pq.read_schema(file) for file in files]
    if any(schema != schemas[0] for schema in schemas[1:]):
        raise ValueError("Original taxi Parquet schemas differ; explicit reconciliation is required")
    source_rows = sum(pq.ParquetFile(file).metadata.num_rows for file in files)
    valid_rows = 0
    latest: datetime | None = None
    for batch in _arrow_batches(files, columns=TAXI_COLUMNS):
        valid_rows += int(np.count_nonzero(_mask(batch, zones)))
        for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
            value = pc.max(batch.column(batch.schema.get_field_index(name))).as_py()
            if value is not None and (latest is None or value > latest):
                latest = value
    if latest is None:
        raise ValueError("Taxi source contains no timestamps")
    return source_rows, valid_rows, latest, schemas[0]


def _take_positions(batch: pa.RecordBatch, positions: np.ndarray, start: int) -> pa.Table:
    lo, hi = np.searchsorted(positions, [start, start + batch.num_rows])
    selected = positions[lo:hi] - start
    return pa.Table.from_batches([batch.take(pa.array(selected, type=pa.int32()))])


def _shift_trips(table: pa.Table, first_ordinal: int, anchor: datetime) -> pa.Table:
    if not table.num_rows:
        return table
    pickups = table["tpep_pickup_datetime"].to_pylist()
    dropoffs = table["tpep_dropoff_datetime"].to_pylist()
    shifted_pickups = []
    shifted_dropoffs = []
    for local_index, (old_pickup, old_dropoff) in enumerate(zip(pickups, dropoffs)):
        ordinal = first_ordinal + local_index
        pickup = (anchor + timedelta(days=ordinal % 7, hours=old_pickup.hour,
                                     minutes=old_pickup.minute, seconds=old_pickup.second,
                                     microseconds=old_pickup.microsecond + ordinal))
        shifted_pickups.append(pickup)
        shifted_dropoffs.append(pickup + (old_dropoff - old_pickup))
    for name, values in (("tpep_pickup_datetime", shifted_pickups),
                         ("tpep_dropoff_datetime", shifted_dropoffs)):
        index = table.schema.get_field_index(name)
        table = table.set_column(index, name, pa.array(values, type=table.schema.field(index).type))
    return table


def generate_taxi(source: Path, output: Path, zones: set[int], *, seed: int,
                  new_ratio: float = .075, duplicate_ratio: float = .015) -> dict[str, Any]:
    files = sorted(source.glob("yellow_tripdata_2024-*.parquet"))
    if not files:
        raise FileNotFoundError(f"No yellow_tripdata_2024-*.parquet in {source}")
    source_rows, eligible_rows, latest, schema = _taxi_profile(files, zones)
    new_count = round(source_rows * new_ratio)
    duplicate_count = round(source_rows * duplicate_ratio)
    if new_count + duplicate_count > eligible_rows:
        raise ValueError("Too few eligible source trips to make requested release")
    anchor = (latest + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if anchor >= datetime(2100, 1, 1):
        raise ValueError("Latest raw taxi timestamp leaves no valid future release date")
    rng = np.random.default_rng(seed)
    sampled = rng.choice(eligible_rows, size=new_count + duplicate_count, replace=False)
    new_positions = np.sort(sampled[:new_count])
    duplicate_positions = np.sort(sampled[new_count:])
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_suffix(".partial.parquet")
    writer = pq.ParquetWriter(partial, schema=schema, compression="snappy")
    seen_eligible = written_new = written_duplicate = 0
    try:
        for batch in _arrow_batches(files):
            filtered = batch.filter(pa.array(_mask(batch, zones)))
            if filtered.num_rows == 0:
                continue
            new = _take_positions(filtered, new_positions, seen_eligible)
            if new.num_rows:
                new = _shift_trips(new, written_new, anchor)
                writer.write_table(new)
                written_new += new.num_rows
            duplicate = _take_positions(filtered, duplicate_positions, seen_eligible)
            if duplicate.num_rows:
                writer.write_table(duplicate)
                written_duplicate += duplicate.num_rows
            seen_eligible += filtered.num_rows
    finally:
        writer.close()
    if (written_new, written_duplicate, seen_eligible) != (new_count, duplicate_count, eligible_rows):
        partial.unlink(missing_ok=True)
        raise AssertionError("Taxi stream counts changed between passes")
    os.replace(partial, output)
    update_pickups = pq.read_table(output, columns=["tpep_pickup_datetime"])["tpep_pickup_datetime"]
    return {
        "file": output.name, "input_rows": source_rows,
        "eligible_source_rows": eligible_rows, "new_records": written_new,
        "duplicate_records": written_duplicate, "modified_records": 0,
        "total_records": written_new + written_duplicate,
        "source_latest_timestamp": latest.isoformat(sep=" "),
        "update_time_range": [pc.min(update_pickups).as_py().isoformat(sep=" "),
                              pc.max(update_pickups).as_py().isoformat(sep=" ")],
        "new_pickup_period": [anchor.isoformat(sep=" "),
                              (anchor + timedelta(days=7)).isoformat(sep=" ")],
        "schema": [{"name": field.name, "type": str(field.type), "nullable": field.nullable}
                   for field in schema],
        "schema_changes": [], "sha256": _sha256(output), "bytes": output.stat().st_size,
    }


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"Empty CSV: {path}")
        return list(reader.fieldnames), list(reader)


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".partial.csv")
    with partial.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n",
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(partial, path)


def _weather_ts(row: dict[str, str]) -> datetime | None:
    try:
        return datetime(*(int(row[key]) for key in ("year", "month", "day", "hour")))
    except (ValueError, KeyError, TypeError):
        return None


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _number(value: str | None, fallback: float) -> float:
    try:
        result = float(value or "")
        return result if math.isfinite(result) else fallback
    except ValueError:
        return fallback


def generate_weather(source: Path, output: Path, *, seed: int, hours: int = 168,
                     revisions: int = 6) -> dict[str, Any]:
    fields, rows = _read_csv(source / "weather.csv")
    time_rows = sorted(((_weather_ts(row), row) for row in rows if _weather_ts(row) is not None),
                       key=lambda item: item[0])
    if not time_rows:
        raise ValueError("Weather source has no valid hours")
    latest = time_rows[-1][0]
    last_by_hour = {timestamp.hour: row for timestamp, row in time_rows[-24 * 14:]}
    if len(last_by_hour) != 24:
        raise ValueError("Weather source lacks a usable 24-hour template")
    rng = Random(seed + 1)
    output_rows = []
    for index in range(hours):
        stamp = latest + timedelta(hours=index + 1)
        row = dict(last_by_hour[stamp.hour])
        row.update({"year": str(stamp.year), "month": str(stamp.month),
                    "day": str(stamp.day), "hour": str(stamp.hour)})
        row["temp"] = f"{_clamp(_number(row.get('temp'), 12) + rng.uniform(-1.5, 1.5), -30, 45):.1f}"
        row["rhum"] = str(round(_clamp(_number(row.get("rhum"), 65) + rng.uniform(-6, 6), 20, 100)))
        row["humidity"] = str(round(_clamp(float(row["rhum"]) + rng.uniform(-4, 4), 20, 100)))
        row["prcp"] = f"{_clamp(_number(row.get('prcp'), 0), 0, 1000):.1f}"
        for field in fields:
            if field.endswith("_source"):
                row[field] = "simulated_week3"
        output_rows.append(row)
    historic = [row for stamp, row in time_rows
                if datetime(2024, 2, 1) <= stamp < datetime(2024, 4, 1)
                and row.get("temp") and row.get("rhum")]
    if len(historic) < revisions:
        raise ValueError("Weather source lacks enough February–March rows to revise")
    for original in rng.sample(historic, revisions):
        row = dict(original)
        row["temp"] = f"{_clamp(_number(row['temp'], 12) + 1.2, -30, 45):.1f}"
        row["temp_source"] = "simulated_week3_revision"
        row["humidity"] = str(round(_clamp(_number(row.get("rhum"), 65), 20, 100)))
        output_rows.append(row)
    _write_csv(output, fields + ["humidity"], output_rows)
    update_stamps = [_weather_ts(row) for row in output_rows]
    return {
        "file": output.name, "input_rows": len(rows), "new_records": hours,
        "duplicate_records": 0, "modified_records": revisions,
        "total_records": len(output_rows),
        "source_latest_timestamp": latest.isoformat(sep=" "),
        "update_time_range": [min(update_stamps).isoformat(sep=" "),
                              max(update_stamps).isoformat(sep=" ")],
        "new_observation_period": [(latest + timedelta(hours=1)).isoformat(sep=" "),
                                   (latest + timedelta(hours=hours)).isoformat(sep=" ")],
        "schema": fields + ["humidity"],
        "schema_changes": [{"column": "humidity", "type": "integer percentage", "range": [20, 100]}],
        "sha256": _sha256(output), "bytes": output.stat().st_size,
    }


def _ny_local(utc_stamp: datetime) -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return utc_stamp.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("America/New_York"))
    except Exception:  # Windows Python without tzdata: use US DST boundaries.
        march_1 = datetime(utc_stamp.year, 3, 1)
        november_1 = datetime(utc_stamp.year, 11, 1)
        second_sunday = 1 + (6 - march_1.weekday()) % 7 + 7
        first_sunday = 1 + (6 - november_1.weekday()) % 7
        dst_start = datetime(utc_stamp.year, 3, second_sunday, 7)
        dst_end = datetime(utc_stamp.year, 11, first_sunday, 6)
        return utc_stamp - timedelta(hours=4 if dst_start <= utc_stamp < dst_end else 5)


def _aqi(pm25: float) -> int:
    """Approximate US PM2.5 AQI breakpoints, for simulated observations only."""
    for c_lo, c_hi, a_lo, a_hi in (
        (0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150), (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500),
    ):
        if pm25 <= c_hi:
            return round(_clamp(a_lo + (pm25 - c_lo) * (a_hi - a_lo) / (c_hi - c_lo), 0, 500))
    return 500


def generate_air_quality(source: Path, output: Path, *, seed: int,
                         hours: int = 168) -> dict[str, Any]:
    path = source / "air_quality" / "hourly_88101_2024.csv"
    rng = Random(seed + 2)
    latest: datetime | None = None
    county_templates: dict[str, dict[str, str]] = {}
    correction_templates: dict[str, dict[str, str]] = {}
    source_rows = 0
    latest_key = ""
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        fields = next(reader)
        indices = {name: index for index, name in enumerate(fields)}
        needed = ("Date GMT", "Time GMT", "State Code", "County Code")
        if any(name not in indices for name in needed):
            raise ValueError("Air-quality source misses its date or station columns")
        for values in reader:
            source_rows += 1
            if len(values) != len(fields):
                continue
            date = values[indices["Date GMT"]]
            hour = values[indices["Time GMT"]]
            key = f"{date} {hour}"
            if key > latest_key:
                try:
                    candidate = datetime.strptime(key, "%Y-%m-%d %H:%M")
                except ValueError:
                    continue
                latest, latest_key = candidate, key
            county = values[indices["County Code"]].zfill(3)
            if (values[indices["State Code"]].zfill(2) != "36" or county not in NYC_COUNTIES):
                continue
            try:
                datetime.strptime(key, "%Y-%m-%d %H:%M")
            except ValueError:
                continue
            row = dict(zip(fields, values))
            if (row.get("Parameter Name") != "PM2.5 - Local Conditions"
                    or row.get("Parameter Code") != "88101"
                    or not row.get("Site Num", "").isdigit()
                    or not row.get("POC", "").isdigit()
                    or not -90 <= _number(row.get("Latitude"), -999) <= 90
                    or not -180 <= _number(row.get("Longitude"), -999) <= 180
                    or not 0 <= _number(row.get("Sample Measurement"), -1) <= 1000):
                continue
            old = county_templates.get(county)
            if old is None or (row["Date GMT"], row["Time GMT"]) > (old["Date GMT"], old["Time GMT"]):
                county_templates[county] = dict(row)
            if ("2024-02-01" <= row["Date GMT"] < "2024-04-01"
                    and county not in correction_templates):
                correction_templates[county] = dict(row)
    if latest is None or not county_templates:
        raise ValueError("Air-quality source lacks valid UTC time or NYC stations")
    output_rows: list[dict[str, str]] = []
    for index in range(hours):
        stamp = latest + timedelta(hours=index + 1)
        local = _ny_local(stamp)
        for county in sorted(county_templates):
            row = dict(county_templates[county])
            baseline = _number(row.get("Sample Measurement"), 10)
            pm25 = round(_clamp(baseline + rng.uniform(-5, 5), 0, 150), 1)
            row.update({
                "Date GMT": stamp.strftime("%Y-%m-%d"), "Time GMT": stamp.strftime("%H:%M"),
                "Date Local": local.strftime("%Y-%m-%d"), "Time Local": local.strftime("%H:%M"),
                "Sample Measurement": f"{pm25:.1f}",
                "Date of Last Change": (stamp + timedelta(days=1)).strftime("%Y-%m-%d"),
                "aqi": str(_aqi(pm25)),
            })
            output_rows.append(row)
    for county in sorted(correction_templates):
        row = dict(correction_templates[county])
        pm25 = round(_clamp(_number(row["Sample Measurement"], 10) + 2.5, 0, 500), 1)
        row["Sample Measurement"] = f"{pm25:.1f}"
        row["Date of Last Change"] = (latest + timedelta(days=1)).strftime("%Y-%m-%d")
        row["aqi"] = str(_aqi(pm25))
        output_rows.append(row)
    _write_csv(output, fields + ["aqi"], output_rows)
    update_stamps = [datetime.strptime(f"{row['Date GMT']} {row['Time GMT']}", "%Y-%m-%d %H:%M")
                     for row in output_rows]
    return {
        "file": output.name, "input_rows": source_rows,
        "new_records": hours * len(county_templates), "duplicate_records": 0,
        "modified_records": len(correction_templates), "total_records": len(output_rows),
        "source_latest_timestamp": latest.isoformat(sep=" "),
        "update_time_range": [min(update_stamps).isoformat(sep=" "),
                              max(update_stamps).isoformat(sep=" ")],
        "new_observation_period": [(latest + timedelta(hours=1)).isoformat(sep=" "),
                                   (latest + timedelta(hours=hours)).isoformat(sep=" ")],
        "stations_generated": sorted(county_templates),
        "schema": fields + ["aqi"],
        "schema_changes": [{"column": "aqi", "type": "integer index", "range": [0, 500]}],
        "sha256": _sha256(output), "bytes": output.stat().st_size,
    }


def generate_zones(source: Path, output: Path) -> dict[str, Any]:
    fields, rows = _read_csv(source / "taxi_zone_lookup.csv")
    by_id = {int(row["LocationID"]): row for row in rows if row.get("LocationID", "").isdigit()}
    chosen = [key for key in (132, 161, 230) if key in by_id]
    if len(chosen) < 3:
        chosen.extend(key for key in sorted(by_id) if key not in chosen and len(chosen) < 3)
    revisions = []
    for location_id in chosen:
        row = dict(by_id[location_id])
        row["Zone"] = f"{row['Zone']} (2024 update)"
        revisions.append(row)
    _write_csv(output, fields, revisions)
    return {
        "file": output.name, "input_rows": len(rows), "new_records": 0,
        "duplicate_records": 0, "modified_records": len(revisions),
        "total_records": len(revisions), "changed_location_ids": chosen,
        "schema": fields, "schema_changes": [],
        "sha256": _sha256(output), "bytes": output.stat().st_size,
    }


def generate_release(source: Path, output: Path, *, seed: int = 2221,
                     new_ratio: float = .075, duplicate_ratio: float = .015,
                     hours: int = 168) -> dict[str, Any]:
    """Create all four updates; return/write a deterministic manifest."""
    source = source.resolve()
    output = output.resolve()
    if not source.is_dir():
        raise FileNotFoundError(source)
    _, zone_rows = _read_csv(source / "taxi_zone_lookup.csv")
    invalid_text = {"", "N/A", "NA", "NULL"}
    known_zones = {
        int(row["LocationID"])
        for row in zone_rows
        if row.get("LocationID", "").isdigit()
        and row.get("Borough", "").strip().upper() not in invalid_text
        and row.get("Zone", "").strip().upper() not in invalid_text
    }
    manifest = {
        "format_version": 1, "seed": seed,
        "timezone_assumptions": {"taxi": "America/New_York source wall time",
                                 "weather": "UTC, unverified source assumption",
                                 "air_quality": "Date GMT / Time GMT"},
        "datasets": {
            "taxi_trips": generate_taxi(source, output / "taxi_trips.parquet", known_zones,
                                         seed=seed, new_ratio=new_ratio,
                                         duplicate_ratio=duplicate_ratio),
            "weather": generate_weather(source, output / "weather.csv", seed=seed, hours=hours),
            "air_quality": generate_air_quality(source, output / "air_quality.csv", seed=seed, hours=hours),
            "taxi_zone_lookup": generate_zones(source, output / "taxi_zone_lookup.csv"),
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    partial = output / "manifest.partial.json"
    partial.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(partial, output / "manifest.json")
    return manifest
