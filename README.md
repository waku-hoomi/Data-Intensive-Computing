# ID2221 Week 1 — Urban Data Integration Platform

A PySpark + Delta Lake platform that ingests NYC taxi trips, weather, air quality,
and taxi zone lookup data through a single generic, config-driven ingestion
framework, then produces an integrated analytical Delta table
(`integrated_taxi_trips`). See `docs/DESIGN_REPORT.md`, `docs/DATA_CATALOG.md`,
`docs/ARCHITECTURE.md`, and `docs/BENCHMARK_REPORT.md` for the full write-up.

## Requirements

- Python 3.9+ (tested on 3.9.13)
- Java 17 (tested on Amazon Corretto 17)
- ~2GB free disk for `dataset/` + generated `data/` (bronze + gold + benchmark)

## Setup

```bash
pip install -r requirements.txt
```

Installs `pyspark==4.0.4`, `delta-spark==4.0.1`, plus `pandas`/`pyarrow`/`pyyaml`.

### Windows only: Hadoop native libraries

Spark on Windows needs `winutils.exe` and `hadoop.dll` on the PATH (Hadoop's
Windows filesystem shim — without them, Delta table writes fail with
`UnsatisfiedLinkError` or `HADOOP_HOME and hadoop.home.dir are unset`). This repo
expects them at `./hadoop/bin/winutils.exe` and `./hadoop/bin/hadoop.dll`
(matching the bundled PySpark's Hadoop 3.3.x client). `src/urban_platform/utils/
spark_session.py` auto-detects this directory and sets `HADOOP_HOME`/`PATH` for
you — you only need to place the two files there once. They are small,
widely-mirrored open-source binaries (not part of this submission's own code);
if missing, obtain them from any `hadoop-3.3.6/bin` winutils mirror.

Linux/macOS: no extra setup needed.

## Data

Place the raw source files under `dataset/` (git-ignored, not included in this
submission due to size):

```
dataset/
  taxi_zone_lookup.csv
  weather.csv
  air_quality/hourly_88101_2024.csv
  yellow_tripdata_2024-01.parquet
  yellow_tripdata_2024-02.parquet
  yellow_tripdata_2024-03.parquet
```

Sources: NYC TLC trip records (Jan–Mar 2024 Yellow Taxi), NOAA/Meteostat hourly
weather for NYC 2024, EPA AQS hourly PM2.5 (`hourly_88101_2024.csv`), TLC taxi
zone lookup table. See `docs/DATA_CATALOG.md` for exact row counts and schemas.

## Running the platform

All commands run from the project root (`ID2221/`).

```bash
# 1. Ingest every dataset into data/bronze/ as Delta tables (Task 3)
python scripts/run_ingestion.py

#    ...or ingest a single dataset by name:
python scripts/run_ingestion.py taxi_trips

# 2. Build the integrated analytical table into data/gold/ (Task 5)
python scripts/run_integration.py

# 3. Run the Task 6 storage-strategy benchmark
python scripts/run_benchmark.py
```

Each script prints a per-dataset summary (rows in/out/rejected, execution time,
output path) to stdout and appends a structured record to
`data/_metadata/ingestion_log.jsonl` (ingestion) or writes
`data/_metadata/benchmark_results.json` (benchmark).

### Expected output (measured on the reference dataset — see docs for details)

| Dataset | Input rows | Output rows | Rejected | Time |
|---|---:|---:|---:|---:|
| taxi_zone_lookup | 265 | 265 | 0 | ~16s |
| weather | 8,784 | 8,784 | 0 | ~4–25s |
| air_quality | 8,139,551 | 7,928,224 | 211,327 | ~17–41s |
| taxi_trips | 9,554,778 | 9,418,044 | 136,734 | ~33–59s |

`integrated_taxi_trips`: 9,418,044 rows; 0.00% missing weather, 90.05% missing
air quality (Manhattan/Staten Island have no PM2.5 monitoring stations in the
source data — see `docs/DESIGN_REPORT.md` §4 for why this is a real coverage
gap, not a join bug).

## Project layout

```
config/datasets.yaml         # declarative registry: one entry per dataset
src/urban_platform/
  utils/                     # Spark session bootstrap, config loader
  transform/transforms.py    # the ONLY dataset-specific code (one fn per dataset)
  ingestion/                 # generic pipeline: load, validate, quality-check, write, log
  integration/integrate.py   # Task 5: builds integrated_taxi_trips
  benchmark/                 # Task 6: storage-strategy benchmark
scripts/                     # thin CLI wrappers around the above
docs/                        # DATA_CATALOG, DESIGN_REPORT, ARCHITECTURE, BENCHMARK_REPORT
data/                        # generated: bronze/, gold/, _metadata/ (git-ignored)
hadoop/                      # Windows-only native libs (git-ignored, see Setup)
```

## Adding a new dataset

1. Add one entry to `config/datasets.yaml`: input format/path, primary key,
   timestamp columns, numeric validation rules, partitioning.
2. If the raw shape needs real reshaping (renames, timezone handling,
   filtering), add one small function to `transform/transforms.py` and point
   `transform_fn` at it.
3. Run `python scripts/run_ingestion.py <new_dataset_name>`.

No changes to `ingestion/pipeline.py` or `ingestion/quality.py` are required —
see `docs/DESIGN_REPORT.md` §3 for why.