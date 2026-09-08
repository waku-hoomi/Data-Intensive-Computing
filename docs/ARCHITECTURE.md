# Architecture Diagram (Task: "A diagram of the architecture")

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