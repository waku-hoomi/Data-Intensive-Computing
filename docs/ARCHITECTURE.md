# Platform Architecture

[Project overview](../README.md) · [Runbook](RUNBOOK.md) · [Documentation index](README.md)

The platform grows from source integration in Week 1 to SQL products in Week 2,
incremental maintenance in Week 3 and demand modelling in Week 4. It runs on local
Spark with Delta Lake; raw source files remain read-only.

## Current platform: Weeks 1–4

```mermaid
flowchart TB
    RAW["Raw CSV / Parquet<br/>Taxi, weather, air quality, zones"]
    UPDATES["Deterministic update release<br/>New rows, duplicates, corrections, additive fields"]
    CFG["YAML contracts<br/>Schema, keys, ranges, timestamps, foreign keys"]
    INGEST["PySpark ingestion and transforms<br/>Normalize types, names and timestamps; validate rows"]
    QUAR["Delta quarantine<br/>Invalid rows and rejection reasons"]
    BRONZE["Bronze Delta tables<br/>Validated source data; MERGE for updates"]
    GOLD["Gold Delta table<br/>Trips enriched with zones, weather and air quality"]
    CDF["Change Data Feed<br/>Changes since the last successful publication"]
    REFRESH["Affected trip keys and product groups<br/>Recover pending work; skip irrelevant changes"]
    PRODUCTS["Four versioned Delta products<br/>Daily, zone, weather and air-quality summaries"]
    PUB["Published snapshot manifest<br/>Gold and product versions for a successful run"]
    SQL["Six Spark SQL analyses<br/>Original / optimized result comparisons"]
    ML["Zone-hour demand dataset<br/>Chronological train / validation / test split"]
    MODEL["Spark ML PipelineModel<br/>Feature transforms + GBTRegressor"]
    MON["Delta execution events<br/>Four operational monitoring queries"]

    RAW --> INGEST
    UPDATES --> INGEST
    CFG -.-> INGEST
    INGEST --> QUAR
    INGEST --> BRONZE
    BRONZE --> GOLD
    BRONZE --> CDF
    GOLD --> CDF
    CDF --> REFRESH
    REFRESH --> GOLD
    REFRESH --> PRODUCTS
    GOLD --> PRODUCTS
    GOLD --> PUB
    PRODUCTS --> PUB
    PUB -. "Pinned versions" .-> SQL
    PUB -. "Pinned Gold" .-> ML
    ML --> MODEL
    INGEST -. "Run events" .-> MON
    REFRESH -. "Run events" .-> MON
```

### Data and read boundaries

| Location | Responsibility |
| --- | --- |
| `dataset/` | Original course files; preparation checks the input manifest and records provenance. |
| `data/bronze/` | Standardized and quality-checked source tables. This project uses Bronze / Gold without a separate Silver layer. |
| `data/gold/` | Integrated trips; UTC storage, New York local-time analysis. |
| `data/products/` | Reusable daily mobility, zone statistics, weather impact and air-quality impact summaries. |
| `data/monitoring/` | Execution events and publication manifests containing table versions. |
| `data/quarantine/` | Per-run rejected rows with reasons. |
| `data/ml/` | Training datasets and persisted feature/model pipelines. |
| `evidence/`, `artifacts/week3/` | Committed experiment snapshots; new runtime outputs remain local. |

Analytical readers resolve the latest successful publication and read the recorded
versions. Individual Delta table commits are transactional; the publication
manifest provides the reader's cross-table snapshot boundary. It does not make
all underlying table writes a single multi-table transaction.

### Incremental refresh and recovery

Bronze updates use dataset-specific keys: taxi IDs are full-row hashes, while
weather, air quality and zone keys support corrections. An altered taxi payload
cannot reliably be identified as a correction without a source trip ID.

CDF recovers changes since the last successful publication, including earlier
pending changes when another batch arrives. The runner propagates affected keys
to Gold and recomputes the necessary product groups. Weather products are fully
rebuilt when the analysis window changes their observed-hour denominator.
Successful duplicate replay leaves the analytical state unchanged.

If an update fails after a table commit, readers retain the previous published
versions while a retry completes pending work. Recovery assumes **one writer**,
available source files and retained Delta/CDF history. See the
[runner](../src/urban_platform/incremental/runner.py),
[publication module](../src/urban_platform/monitoring/publication.py) and
[fault-injection tests](../tests/test_week3_recovery.py).

### ML as a downstream consumer

The demand dataset reuses the platform's analysis views and pinned Gold reads.
It preserves zero-demand zone-hours and uses fixed chronological split dates.
Feature transformations and `GBTRegressor` are fitted and saved as one
`PipelineModel`. The published Week 4 results use a strict bootstrap baseline
before incremental updates; additive humidity/AQI columns are unavailable on
that baseline and are excluded from model features. See the
[ML design report](Week4_Design_Report.md).

## Historical Week 1 ingestion diagram

The original diagram below describes the initial full-load design. Its pipeline
ends at Gold and the storage benchmark; use the current diagram above for the
Week 2–4 extensions. Later validation rules and incremental behavior supersede
the initial ingestion assumptions.

```mermaid
flowchart TB
    subgraph raw["dataset/  (raw, read-only)"]
        R1["taxi_zone_lookup.csv"]
        R2["weather.csv"]
        R3["air_quality/hourly_88101_2024.csv"]
        R4["yellow_tripdata_2024-{01,02,03}.parquet"]
    end

    subgraph config["config/datasets.yaml"]
        CFG["one declarative entry per dataset:<br/>input format, primary key,<br/>timestamp cols, numeric rules,<br/>partitioning"]
    end

    subgraph ingestion["Generic Ingestion Framework (Task 3)"]
        LOAD["1. Load raw file<br/>(CSV / Parquet)"]
        VALID["2. Validate raw schema<br/>vs. expected_raw_columns"]
        XFORM["3. Dataset-specific transform()<br/>rename -> snake_case, cast types,<br/>normalize timestamps to UTC"]
        QUALITY["4. Generic quality checks<br/>missing PK, invalid timestamp,<br/>out-of-range numeric, duplicate PK"]
        WRITE["5. Write partitioned Delta table"]
        META["6. Append run metadata<br/>(rows in/out/rejected, time, schema_version)"]
        LOAD --> VALID --> XFORM --> QUALITY --> WRITE --> META
    end

    CFG -.drives.-> LOAD
    CFG -.drives.-> VALID
    CFG -.drives.-> XFORM
    CFG -.drives.-> QUALITY
    CFG -.drives.-> WRITE

    R1 --> LOAD
    R2 --> LOAD
    R3 --> LOAD
    R4 --> LOAD

    subgraph bronze["data/bronze/  (Delta, common data model)"]
        B1["taxi_zone_lookup<br/>(unpartitioned, lookup)"]
        B2["weather<br/>(unpartitioned)"]
        B3["air_quality<br/>(obs_year, obs_month)"]
        B4["taxi_trips<br/>(pickup_year, pickup_month)"]
    end

    META --> B1
    META --> B2
    META --> B3
    META --> B4

    subgraph integration["Integration Pipeline (Task 5)"]
        JOIN1["join pickup/dropoff zone + borough<br/>on LocationID"]
        JOIN2["join weather on<br/>date_trunc(hour, pickup_ts)"]
        JOIN3["aggregate air_quality to (borough, hour),<br/>join on (pickup_borough, pickup_hour)"]
        JOIN1 --> JOIN2 --> JOIN3
    end

    B1 --> JOIN1
    B4 --> JOIN1
    B2 --> JOIN2
    B3 --> JOIN3

    subgraph gold["data/gold/  (Delta)"]
        G1["integrated_taxi_trips<br/>(pickup_year, pickup_month)"]
    end

    JOIN3 --> G1

    subgraph bench["Benchmark (Task 6)"]
        BENCH["Two storage strategies for taxi_trips:<br/>by_month vs. by_borough<br/>-> ingestion time, storage size,<br/>file count, query latency"]
    end

    B4 --> BENCH
    B1 --> BENCH
```

## Reading this diagram

- **Left to right / top to bottom is the data flow**: raw files never move or
  mutate — every stage reads its input and writes a new artifact.
- **`config/datasets.yaml` drives the entire ingestion box** — none of the five
  ingestion steps hardcode a dataset name; they all read their behavior
  (which columns to expect, how to partition, what counts as invalid) from
  this one file, dispatching to a small per-dataset transform function only
  at step 3.
- **Bronze is the boundary where the common data model becomes true.** Every
  bronze table, regardless of source format or naming convention, is
  guaranteed to have `snake_case` columns, UTC timestamps, and NULL (never
  sentinel) missing values from this point on.
- **Gold is derived, not authoritative.** `integrated_taxi_trips` can always be
  rebuilt from bronze; it exists purely to save downstream analytical queries
  (Week 2) from repeating the same three joins.
