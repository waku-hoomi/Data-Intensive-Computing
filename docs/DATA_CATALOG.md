# Data Catalog (Task 1)

Scope: NYC Yellow Taxi trips (Jan–Mar 2024, 3 monthly Parquet files), NOAA/Meteostat
hourly weather for NYC (2024, CSV), EPA AQS hourly PM2.5 air quality (2024, CSV,
nationwide — filtered to New York State boroughs during integration), and the TLC
taxi zone lookup table (CSV). All row counts below are measured from the actual
files in `dataset/`, not estimated.

## 1. `taxi_trips` (NYC Yellow Taxi trip records)

| | |
|---|---|
| **Primary entity** | A single taxi trip (one passenger pickup → drop-off event) |
| **Source rows (3 files, Jan–Mar 2024)** | 9,554,778 |
| **Primary key** | None in the source. TLC trip records carry no trip identifier — a trip is only implicitly identified by the full combination of its fields. We derive a surrogate `trip_id = sha256(all raw columns)` at ingestion time (deterministic, so re-ingesting the same file yields identical IDs — important for idempotent reruns in Week 3). |
| **Join attributes** | `PULocationID`, `DOLocationID` → `taxi_zone_lookup.LocationID`; `tpep_pickup_datetime` (truncated to hour) → `weather.observation_ts` and, via pickup borough, → `air_quality` |
| **Temporal attributes** | `tpep_pickup_datetime`, `tpep_dropoff_datetime` — local America/New_York wall-clock time per the TLC data dictionary (not UTC, not tagged with a timezone in the file itself) |
| **Categorical attributes** | `VendorID`, `RatecodeID`, `payment_type`, `store_and_fwd_flag`, `PULocationID`/`DOLocationID` (categorical location codes, not continuous) |
| **Grows over time** | Yes — this is the fact table. One file per month; the municipality's real feed would append ~3M rows/month at this rate. This drives the partitioning strategy (Task 2). |

## 2. `weather` (hourly weather observations)

| | |
|---|---|
| **Primary entity** | One weather observation for New York City at a given hour |
| **Source rows** | 8,784 (366 days × 24 hours — 2024 is a leap year; confirms one row per hour, no gaps in the raw file) |
| **Primary key** | `(year, month, day, hour)` → standardized to a single `observation_ts` timestamp |
| **Join attributes** | `observation_ts` (truncated to hour) ↔ `taxi_trips.pickup_ts` truncated to hour |
| **Temporal attributes** | `year, month, day, hour` (source), collapsed to `observation_ts` (standardized) |
| **Categorical attributes** | `coco` (weather condition code — categorical, not continuous, despite being numeric) |
| **Grows over time** | Yes, but slowly and predictably — exactly one new row per hour. Unlike `taxi_trips`, this is not a "very large fact table"; it never needs the same partitioning treatment. |

## 3. `air_quality` (EPA AQS hourly PM2.5, `hourly_88101_2024.csv`)

| | |
|---|---|
| **Primary entity** | One PM2.5 measurement from one EPA monitoring station at one hour |
| **Source rows** | 8,139,551 (nationwide, all EPA AQS monitoring stations for parameter code 88101 / PM2.5) |
| **Rows relevant to this project (New York State)** | 117,438 (filtered by `State Name == 'New York'`) — but even within NY, only 3 NYC boroughs have a station: **Bronx** (17,404), **Kings/Brooklyn** (17,460), **Queens** (17,021). The remaining NY rows are upstate counties (Erie, Monroe, Onondaga, Albany, Essex, Steuben) — not part of NYC and not usable for this integration. **Manhattan and Staten Island have zero monitoring stations in this dataset.** This is a hard coverage gap in the source data (see Task 5). |
| **Primary key** | `(State Code, County Code, Site Num, Parameter Code, POC, Date GMT, Time GMT)` — we standardize the first three into a single `station_id`, giving PK = `(station_id, parameter_code, poc, observation_ts)` |
| **Join attributes** | `County Name` → taxi zone `Borough` (with one naming mismatch: EPA's `Kings` = TLC's `Brooklyn`, handled explicitly in the integration transform); `observation_ts` (truncated to hour) → `taxi_trips.pickup_ts` |
| **Temporal attributes** | `Date Local`/`Time Local` and `Date GMT`/`Time GMT` (both provided; we standardize on the GMT/UTC pair as the source of truth) |
| **Categorical attributes** | `Parameter Name`, `Units of Measure`, `Method Type`, `Qualifier`, `State Name`, `County Name` |
| **Grows over time** | Yes, and at a rate the platform must plan for: one row per station per hour, times however many pollutants/parameter codes the municipality tracks (this file is PM2.5 only — a real deployment would add PM10, O3, NO2, CO, etc., multiplying row count per new pollutant added). |

## 4. `taxi_zone_lookup` (TLC taxi zone lookup table)

| | |
|---|---|
| **Primary entity** | A named NYC taxi zone (a small geographic area used by TLC to bucket pickups/dropoffs) |
| **Source rows** | 265 |
| **Primary key** | `LocationID` |
| **Join attributes** | `LocationID` ← `taxi_trips.PULocationID` / `DOLocationID` (this table exists purely to be joined against; it produces no rows of its own in any analysis) |
| **Temporal attributes** | None |
| **Categorical attributes** | `Borough`, `Zone`, `service_zone` — every column is categorical |
| **Grows over time** | No, or negligibly. TLC redraws zone boundaries only rarely (the last major revision was years ago). This is the textbook example of a **lookup table** (Task 2): small, slow-changing, read-mostly, safe to broadcast-join. |

## Cross-cutting observations that shaped the design

- **Only `taxi_trips` is a genuine "very large fact table."** `weather` and `air_quality` are also time-series fact data, but at radically different scale (8.8K and ~130K NY rows vs. 9.5M). Treating all three with the same partitioning strategy would be a mistake (see Task 2).
- **Timestamps are inconsistent across sources by design, not by accident**: taxi trips are in local NYC time, weather/air-quality are already in UTC. The common data model (Task 4) standardizes all of them to UTC at ingestion time specifically to remove this asymmetry once, rather than at every downstream query.
- **`air_quality`'s real primary key spans 3 raw ID columns** (`State Code`, `County Code`, `Site Num`) that only make sense combined — a good example of why "which attributes uniquely identify a record" sometimes requires a derived key, not a raw column.