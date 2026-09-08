# Design Report — Urban Data Integration Platform (Week 1)

## 1. Storage Architecture (Task 2)

### Directory structure

```
ID2221/
  dataset/                         # raw, untouched, read-only municipality drop zone
    taxi_zone_lookup.csv
    weather.csv
    air_quality/hourly_88101_2024.csv
    yellow_tripdata_2024-01.parquet
    yellow_tripdata_2024-02.parquet
    yellow_tripdata_2024-03.parquet
  data/
    bronze/                        # one Delta table per source dataset, standardized schema
      taxi_zone_lookup/
      weather/
      air_quality/
      taxi_trips/
    gold/                          # integrated / derived analytical Delta tables
      integrated_taxi_trips/
    _metadata/                     # ingestion_log.jsonl, benchmark_results.json
  config/datasets.yaml             # declarative dataset registry (Task 3)
  src/urban_platform/              # ingestion / transform / integration / benchmark code
  scripts/                         # thin CLI entry points
```

We use a **Bronze/Gold** split (no separate Silver layer): Bronze already holds the
fully standardized, quality-checked common-data-model output of each source
(Task 4) — there is no messy "raw-as-is" layer to separate from it, because the raw
files themselves (`dataset/`) already serve that role and are never written to.
Gold holds the integrated, enriched, query-ready dataset (Task 5). Adding a
"true Silver" layer would only make sense once per-dataset business logic
(deduplication windows, SCD handling, etc.) grows complex enough to need its own
checkpoint — not needed at this project's scale.

### Delta table organization & naming conventions

- One Delta table per **bronze** dataset, named identically to the dataset's logical
  name in `config/datasets.yaml` (`taxi_trips`, `weather`, `air_quality`,
  `taxi_zone_lookup`) — the config name, the Delta path, and the dataset's identity
  in the ingestion log are always the same string, so nothing needs a lookup table
  to cross-reference.
- All column names are `snake_case` (vs. the source files' mix of `PascalCase`,
  `Title Case With Spaces`, and `lower_snake`), decided once in Task 4's common
  data model and enforced by each dataset's transform function.
- Gold tables are named for what they represent, not how they're built:
  `integrated_taxi_trips`, not `taxi_trips_joined_v2`.

### Partitioning strategy

| Table | Partitioned by | Why |
|---|---|---|
| `taxi_trips` | `pickup_year, pickup_month` | The only genuinely large, ever-growing fact table (9.5M rows for 3 months, and the municipality feeds it one file per month). Partitioning by ingestion cadence means a new month's file becomes a new partition with zero rewrite of old data, and every one of Task 6's benchmark queries plus Week 2's monthly-demand queries can prune to the relevant month(s). |
| `air_quality` | `obs_year, obs_month` | Same reasoning as `taxi_trips`: it is also fed on a rolling schedule and will keep growing. At current scale (117K NY rows) partition pruning barely matters yet, but the partitioning is future-proofed for when EPA data for more years/pollutants is added. |
| `weather` | *(none)* | See "which datasets should not be partitioned" below. |
| `taxi_zone_lookup` | *(none)* | See below — lookup tables are never partitioned. |
| `integrated_taxi_trips` (gold) | `pickup_year, pickup_month` | Inherits the partitioning of its largest input (`taxi_trips`) since it has the same row-count order of magnitude and the same query patterns (mostly time-bounded). |

**Which datasets should conceptually be treated as lookup tables?**
Only `taxi_zone_lookup`. It is small (265 rows), essentially static (TLC revises
zone boundaries rarely), and exists purely to be joined against — it is never
itself the subject of an aggregation. It is the only table we deliberately
broadcast-join rather than partition.

**Which datasets should not be partitioned? Why?**
`taxi_zone_lookup` and `weather`. `taxi_zone_lookup` has too few rows (265) for
partitioning to do anything but create more small files than the table has
rows. `weather` has only 8,784 rows total (one per hour, for one year) — the
entire table is a fraction of a single reasonably-sized Parquet file; splitting
it into per-month partitions would produce dozens of tiny files each holding a
few hundred KB, which is strictly worse for both storage overhead and read
performance than one unpartitioned file. Partitioning is a tool for pruning
large scans, and there is nothing here worth pruning.

**Which datasets require different partitioning strategies?**
`taxi_trips` and `air_quality` share the same *temporal* strategy but at very
different scale factors, and `air_quality` would additionally benefit from a
`state`/`county` (or, post-integration, `borough`) partition if the platform
grows to ingest multiple states/pollutants — something `taxi_trips` (single
city) never needs.

**Under what conditions does partitioning become harmful?**
1. **Too many small partitions relative to data volume** — our own `weather`
   table is the textbook case: partitioning by month would create 12 partitions
   averaging <1MB each, each stored as its own tiny Parquet file. Delta/Spark's
   per-file overhead (footer metadata, open/close cost, driver-side listing)
   then dominates the actual data cost.
2. **High-cardinality partition columns** — our own Task 6 benchmark makes this
   concrete: partitioning `taxi_trips` by `pickup_borough` (a handful of
   distinct values) produced **207 files** for the same ~800MB of data that
   `pickup_year, pickup_month` (3 distinct values) stored in just **30 files**
   (see `BENCHMARK_REPORT.md`). Partitioning by something closer to a primary
   key (e.g. `trip_id`, or a raw `LocationID` with 265 distinct values) would
   make this dramatically worse — potentially thousands of files, each holding
   a handful of rows.
3. **Partition column not used in filters** — if most real queries do not
   filter or group by the partition column, partitioning adds directory-listing
   and metadata overhead on every read with no pruning benefit in return.
4. **Skewed partition sizes** — Manhattan alone is 89.6% of all taxi pickups
   (measured: 8,443,046 of 9,418,044 clean trips). A borough-based partitioning
   scheme would produce one enormous partition and several nearly-empty ones —
   defeating the goal of even, prunable chunks.

**If the total data volume increased by 20×:**
- `taxi_trips` at ~190M rows/quarter would justify a two-level partition
  (`pickup_year, pickup_month`, optionally `pickup_borough` as a third
  sub-partition only within Manhattan-heavy months, since Manhattan's skew is
  the actual bottleneck, not boroughs in general) plus enabling Delta's
  `OPTIMIZE`/Z-ORDER on `PULocationID` for query patterns that don't align with
  the partition key.
- `air_quality` would cross the threshold where per-borough or per-state
  sub-partitioning starts paying for itself.
- `weather` would very likely still not need partitioning — 20× more *years* of
  hourly data for one city is still under 200K rows.
- We would also reconsider file sizing: Delta best practice targets
  128MB–1GB per file; at 20× volume we would explicitly coalesce/repartition
  before write rather than relying on Spark's default output parallelism,
  to avoid the small-files problem observed even at current scale in the
  `by_borough` benchmark run.

## 2. Common Data Model (Task 4)

Every dataset's transform function (`src/urban_platform/transform/transforms.py`)
converts its raw shape into this common model. No exceptions — this is the only
dataset-specific code in the framework; everything else operates purely on the
model's guarantees.

- **Standard timestamp format**: Spark `TimestampType`, stored and interpreted as
  **UTC**. Source data is not consistent about this — NYC TLC trip timestamps are
  local `America/New_York` wall-clock time (per the TLC data dictionary, with no
  timezone marker in the file), while both weather (Meteostat/ISD) and EPA AQS
  ship GMT columns directly. We convert taxi timestamps to UTC with
  `to_utc_timestamp(ts, 'America/New_York')` **at ingestion time**, so every
  downstream join (Task 5) and every later analytical query (Week 2) can compare
  timestamps directly with no per-query timezone reasoning.
- **Naming conventions**: every column is `snake_case`. Source columns arrive as
  `PascalCase` (`LocationID`), `Title Case With Spaces` (`Date Local`), or mixed
  (`tpep_pickup_datetime`) — the transform functions rename all of them once, so
  no downstream SQL/DataFrame code ever has to remember which convention a given
  column came from.
- **Rules for handling missing values**: missing values are always SQL `NULL`,
  never a sentinel (`-999`, empty string, `"N/A"` as a literal string masquerading
  as data). This is enforced by using explicit `.cast(...)` in every transform:
  a value that fails to parse becomes `NULL` rather than a silently-wrong number.
  Missing values are **never imputed** during ingestion or integration — a `NULL`
  air-quality reading for a Manhattan pickup means "no station covers this
  borough," which is real information that imputation would destroy. Consumers
  (Week 2 analytical queries) decide per-query whether to filter or impute.
- **Common data types**: `IntegerType` for identifiers/codes/counts (`vendor_id`,
  `passenger_count`, `location_id`), `DoubleType` for continuous measurements
  (`fare_amount`, `temp_c`, `pm25_ug_m3`), `StringType` for categorical/text
  fields, `TimestampType` for all temporal columns. No `FloatType` (avoids
  precision surprises in aggregation) and no dataset-specific numeric types.

Every transformation is documented as an inline comment at the point it happens
in `transforms.py`, rather than in a separate spec that could drift out of sync
with the code — e.g. the reasoning for treating EPA's `County Name = "Kings"` as
equivalent to TLC's `Borough = "Brooklyn"` lives directly next to the line of
code that performs the substitution (`integrate.py`).

## 3. Ingestion Framework (Task 3)

### Generic vs. dataset-specific components

**Generic (dataset-agnostic, in `ingestion/pipeline.py`, `ingestion/quality.py`,
`ingestion/run_metadata.py`)**:
- File loading (CSV/Parquet) — dispatches purely on `input_format` from config.
- Raw-schema validation — checks the loaded columns against
  `expected_raw_columns` declared in config; fails fast with a clear error if the
  municipality changes a source file's column names, instead of silently
  producing `NULL`s downstream.
- Data-quality checks — missing primary key, invalid/out-of-range timestamps,
  out-of-range numeric values, duplicate primary keys — implemented once against
  the *declared* `primary_key` / `timestamp_cols` / `numeric_rules` in config,
  so they apply identically to every dataset without dataset-specific branches.
- Delta write (partitioned per config) and ingestion metadata logging.

**Dataset-specific (one function per dataset, in `transform/transforms.py`)**:
- Column renaming to the common model's names.
- Type casting rules specific to that source's raw representation.
- Timestamp assembly/timezone conversion specific to that source (e.g. weather's
  `year/month/day/hour` → single timestamp; EPA's `Date GMT + Time GMT` string
  concatenation; TLC's local→UTC conversion).
- Any source-specific filtering (e.g. `air_quality`'s transform filters to
  `Parameter Name == 'PM2.5 - Local Conditions'` before renaming, since the raw
  file is one CSV covering multiple pollutant parameters even though we only
  ingest PM2.5 for this project).

This split exists because *loading a file* and *checking a range* are the same
operation no matter which dataset you're looking at, but *what a raw column
means and how to reshape it* is inherently dataset knowledge that cannot be
inferred generically.

### How transformation rules are defined and maintained

Each dataset's rules live in exactly one place: `config/datasets.yaml` for
declarative rules (primary key, timestamp columns, numeric ranges, partitioning),
and one function in `transforms.py` for the reshaping logic itself. There is no
second copy of "what does the taxi_trips schema look like" anywhere else in the
codebase — the ingestion pipeline, the quality checks, and the Delta writer all
derive their behavior from this single config entry at runtime
(`DatasetConfig.resolve_transform_fn()` dynamically imports the declared
function).

### How metadata is managed

Every ingestion run appends one JSON record to `data/_metadata/ingestion_log.jsonl`
(`ingestion/run_metadata.py`): dataset name, start/end time, execution seconds,
schema version (currently a single global integer in `datasets.yaml`, bumped
whenever the common data model changes), input/output/rejected row counts, and
a breakdown of rejections by reason. This is append-only and one line per run,
so the file itself is a complete audit trail of every ingestion the platform has
ever performed — this is deliberately the format Week 3's incremental-update
work will need to answer "what changed since the last run."

### How this design reduces duplication and simplifies maintenance

Before this design, adding a dataset would mean writing a full load→validate→
transform→check→write→log script from scratch, with every dataset's script
reimplementing the same duplicate-detection window logic, the same range-check
pattern, the same Delta-write boilerplate. Here, `ingestion/pipeline.py` and
`ingestion/quality.py` are the *only* place that logic exists; every dataset
gets it for free by declaring its config. A bug fix or new check (e.g. "also
reject rows with a null geometry") is written once and instantly applies to all
four current datasets and any future ones.

### If the municipality adds 20 new datasets next year

For a dataset that fits the existing shape (tabular CSV/Parquet, a primary key,
some timestamps, some numeric ranges): add one block to `datasets.yaml` and,
only if the raw shape needs real reshaping (renames, timezone handling,
filtering), one new transform function of a few lines. Zero changes to
`pipeline.py` or `quality.py`. The only framework-level extension this would
plausibly require is adding a new `input_format` branch (e.g. JSON, a
streaming source, or an API pull) if one of the 20 doesn't arrive as a flat
file — everything downstream of `_load_raw()` already only depends on having a
Spark DataFrame with the expected columns, not on the file format.

## 4. Integration Strategy (Task 5)

`integrated_taxi_trips` enriches every clean taxi trip with pickup-time weather,
pickup-time air quality, pickup zone/borough, and dropoff zone/borough
(`src/urban_platform/integration/integrate.py`).

**How an hourly weather observation is associated with a taxi trip.**
Weather in this dataset is a single citywide series with no location dimension
(one row per hour, for all of NYC) — there is no "which weather station" question
to answer. We truncate each trip's pickup timestamp down to the enclosing hour
(`date_trunc('hour', pickup_ts)`) and join directly on `weather.observation_ts`.
This is an "align to the enclosing hour," not a nearest-neighbour join in either
direction — it says "the weather condition recorded for the hour containing this
pickup," which matches how an hourly observation is conventionally interpreted
(valid for the hour it's timestamped, not a point-in-time reading needing
interpolation).

**How hourly air-quality measurements are associated with a taxi trip.**
Unlike weather, air quality *does* have a location dimension (per-station), but
taxi trips only carry a pickup zone/borough, not GPS coordinates, and the
source EPA data covers only 3 of NYC's 5 boroughs (Bronx, Brooklyn, Queens —
verified against the raw file, see `DATA_CATALOG.md`). We therefore aggregate to
**(borough, hour)** granularity — averaging PM2.5 across every station active in
that borough that hour — and join taxi trips on `(pickup_borough, pickup_hour)`.
This is a deliberately coarser join than weather's (borough-level, not
station-level) because station-level is not answerable from the trip data at
all, and because averaging same-borough stations is a defensible way to
represent "typical air quality in this part of the city at this hour" without
fabricating a false sense of precision.

**How missing observations are handled.**
Left as `NULL`, never imputed (see Common Data Model above). Measured on the
actual integrated table: 8 of 9,418,044 trips (0.00%) are missing weather (edge
effects at the very start/end of the covered date range), while 8,481,108
trips (90.05%) are missing air quality — almost entirely because 89.6% of all
pickups are in Manhattan, which this air-quality source does not cover at all.
This is reported honestly rather than backfilled with an interpolated or
borough-neighbor value, because doing so would manufacture data that looks
like a real sensor reading but isn't.

**Limitations of this integration strategy.**
1. Air-quality coverage is fundamentally incomplete for this city (Manhattan
   and Staten Island have zero stations in the source file) — no join strategy
   can recover data that was never collected; a real platform would need to
   either source a second air-quality feed with better borough coverage or
   explicitly document this as an analytical caveat wherever `pm25_ug_m3` is
   used (e.g. Week 2's "relationship between air quality and taxi demand"
   query is only answerable for 3 of 5 boroughs with this data).
2. Both joins associate the *pickup* borough with a *citywide-per-borough*
   average, not the trip's actual route — a trip that crosses from Queens
   (with air-quality coverage) into Manhattan (without) is only tagged with
   Queens' reading, even though most of the trip may occur elsewhere.
3. Weather is single-station-equivalent for the whole city; a trip in Staten
   Island and a trip in the Bronx get the identical weather reading for the
   same hour, even though real conditions can vary across the city's
   geography.
4. The hour-truncation join for weather treats a trip at 08:59 and a trip at
   08:01 identically (both get the 08:00 reading), which can be off by up to
   ~59 minutes from the "true" instantaneous condition at pickup — acceptable
   for hourly-resolution analysis but not for anything requiring sub-hour
   precision.