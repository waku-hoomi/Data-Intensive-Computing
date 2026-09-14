# ID2221 Lab 2: Querying and Optimizing the Urban Data Platform

Implementation in progress, due 2026-09-16. See `docs/LAB2_PROGRESS.md` for current
validation and pending source decisions. The original README and `docs/` reports
describe Lab 1 and are not Lab 2 experiment results.

## Environment

Use Python 3.12, Java 17, PySpark 4.0.4 and Delta Lake 4.0.1. Install the project
requirements into a virtual environment; set `JAVA_HOME` to your JDK directory.
The default local Spark master is `local[4]` with a 4 GB driver request and eight
SQL shuffle partitions. Override `LAB2_SPARK_MASTER` or `LAB2_DRIVER_MEMORY` if
needed and record the change when comparing benchmark results.

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-lab2.txt
python scripts/check_environment.py
```

Windows activation uses `.venv\Scripts\activate`; the existing project contains
optional Windows Hadoop support. All commands below run from this project root.
Allow enough disk for the 2.37 GB uncompressed air-quality CSV, other raw inputs,
Delta outputs, temporary Spark work and caches (at least 15 GB free recommended).
The first Spark start downloads Delta Java dependencies.

## Data and source decision

Use the same six course files as Lab 1. `config/input_manifest.json` records their
Google Drive IDs, byte counts and SHA-256 hashes. If downloaded to one directory:

```sh
python scripts/prepare_local_data.py /path/to/course/files
```

The script verifies inputs and extracts the air-quality archive into `dataset/`.
The existing Lab 1 README lists all raw filenames. Generated data is excluded
from Git and can be regenerated. Do not replace existing course inputs silently.

Weather location/timezone/unit provenance is awaiting the user's course-data
confirmation. `config/analytics.yaml` deliberately disables dependent production
steps until a decision is recorded. Numeric weather codes can be analyzed as
categories without claiming an undocumented descriptive meaning. Any accepted
UTC interpretation must be recorded as an assumption, not as source evidence.

## Run the platform

```sh
# Independent of the pending weather decision:
python scripts/run_lab2_ingestion.py taxi_zone_lookup air_quality taxi_trips
python scripts/validate_platform.py --bronze-only

# After the weather decision is recorded:
python scripts/run_lab2_ingestion.py weather
python scripts/run_integration.py
python scripts/validate_platform.py
python scripts/run_analytics.py
python scripts/build_data_products.py
python scripts/run_lab2_benchmark.py
```

Run one analysis with `python scripts/run_analytics.py --query 01_zone_monthly`.
Benchmark subsets are `--suite techniques` and `--suite queries`.

## Analytical definitions

- Demand is the count of accepted trips at their pickup zone/time.
- Analysis interval is local 2024-01-01 inclusive to 2024-04-01 exclusive.
- Storage timestamps are UTC; reporting dates, months and hours use New York time.
- Distance uses TLC miles; missing distances are excluded from averages and their
  valid sample count is reported.
- Air-quality association uses one borough-hour per observation, not one copy of
  PM2.5 per trip. Correlation is descriptive and does not establish causality.
- Weather demand variation compares mean trips per observed hour, including
  zero-trip zone-hours. At least 24 observed hours are required per weather code;
  at least two eligible codes are required per zone. Zones are the observed zones
  with non-null IDs in this input, not all hypothetical zones.
- Weather categories that never have any citywide trips have unavailable weather
  context in this integrated-data-only product and remain missing.
- Monthly daily averages divide by calendar days in the configured interval.
  Peak-hour averages divide by actual elapsed hour observations, including DST.

## Products and metadata

Four Delta products are in `data/products/`: daily mobility, monthly zone
statistics, weather impacts and borough-hour air-quality context. They are small
unpartitioned summaries, not copies of the full fact table. Metadata is stored in
the Delta table `data/lab2_metadata/product_registry`, including source table and
version, creation and refresh times, schema version/schema, parameters, row count,
active-file count, storage bytes and build time. Regeneration overwrites product
contents and appends an auditable metadata record while preserving creation time.

## Experiments and artifacts

Every pair uses one warm-up and three measured runs per variant. Variant order
alternates; Spark caches are cleared between variants. Explicit cache creation is
timed separately. Measurements are warm-process local timings; operating-system
file caches are not flushed. Compare repeated medians, not invented speedups.

- `artifacts/environment.json`: actual software/runtime environment.
- `artifacts/validation_*.json`: data validation evidence.
- `artifacts/query_results/`: query output and formatted plans.
- `artifacts/products.json`: product generation and storage information.
- `artifacts/benchmarks/`: every run, original/optimized SQL, result equality,
  EXPLAIN FORMATTED and post-execution physical plans.

The pruning experiment deliberately covers a February local-time window. UTC
storage requires both February and March partitions to preserve the final local
evening. The broadcast experiment uses the genuinely small zone lookup table.
AQE is tested with other configuration held fixed. Saved plans must be inspected
before claiming that a requested optimization actually occurred.

## Tests

```sh
python -m pytest tests -q
```

Tests cover cross-state geography, station weighting, malformed values under ANSI,
valid/invalid duplicate priority, deterministic trip IDs, hand-calculated SQL
results, zero-demand hours and the DST boundary. These complement full-data checks;
they do not replace real measurements or imply the final report is complete.
