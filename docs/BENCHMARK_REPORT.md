# Benchmark Report (Task 6)

## Setup

Both strategies write the same source data — bronze `taxi_trips` (9,418,044 clean
rows, Jan–Mar 2024) left-joined with `taxi_zone_lookup` to attach `pickup_borough`
— to a fresh Delta table under `data/_metadata/benchmark/`, then run the same
three queries against each. Environment: single machine, `local[*]` Spark,
`spark.sql.shuffle.partitions=8` (see `README.md` for full spec). Storage size is
measured as total bytes of `.parquet` data files only (Delta transaction-log
JSON excluded, since it is a small, roughly constant, per-write overhead
unrelated to the partitioning choice being tested).

## Strategies evaluated

| | Strategy A: `by_month` | Strategy B: `by_borough` |
|---|---|---|
| Partition column(s) | `pickup_year, pickup_month` | `pickup_borough` |
| Distinct partition values | 3 | 8 (7 real boroughs/EWR + "Unknown"/"N/A" rejects) |
| Rationale | Matches ingestion cadence (one file/month); used by the platform's actual bronze/gold tables | Matches the GROUP BY column of 2 of the 3 benchmark queries |

## Results

| Metric | `by_month` | `by_borough` |
|---|---:|---:|
| Ingestion (write) time | 46.79 s | 8.10 s |
| Storage size | 807.7 MB | 805.6 MB |
| File count | 30 | 207 |
| Query: trips per borough | 1.700 s | 1.048 s |
| Query: avg trip duration per day | 1.557 s | 0.636 s |
| Query: avg fare per borough | 0.876 s | 0.497 s |

(Raw JSON: `data/_metadata/benchmark_results.json`.)

## Discussion

**Storage size is nearly identical (807.7 MB vs. 805.6 MB)** — partitioning
choice does not change how much data there is, only how it's split into files
and directories. This confirms the two strategies are a fair, apples-to-apples
comparison of *layout*, not of compression or encoding.

**File count diverges sharply: 30 vs. 207.** With only 3 time partitions,
Spark's default output parallelism (10 tasks/partition here, based on the
upstream shuffle) produces ~10 files per partition — 30 total, each a healthy
~27MB. With 8 borough partitions of wildly uneven size (Manhattan alone holds
89.6% of all trips — see `DESIGN_REPORT.md`), the same per-partition output
parallelism instead produces small partitions (Staten Island: 220 rows) split
across just as many files as the giant Manhattan partition, and Spark's default
task count per partition compounds this into 207 files overall — many
holding only a few hundred rows. This is the textbook "high-cardinality,
skewed partition column" failure mode described in Task 2: partitioning
overhead (many small files) grows without a proportional pruning benefit,
because the partition boundaries don't correspond to even chunks of data.

**`by_borough` is faster on every query in this benchmark** (1.05s vs. 1.70s;
0.64s vs. 1.56s; 0.50s vs. 0.88s) — but this is a case of the partitioning
happening to align with the query's own `GROUP BY pickup_borough`/derived-date
column, not a general win. Two of the three benchmark queries group by
`pickup_borough` directly, so `by_borough`'s partition pruning and
partition-aligned shuffle avoid work that `by_month` cannot avoid (it must
scan across all 3 month-partitions and still shuffle by borough at query
time). This is exactly Task 2's principle in action: **partition on what
your actual queries filter or group by**, not on ingestion cadence, if query
latency is the dominant cost. The trade-off Task 2 warns about is visible in
the ingestion numbers, not the query numbers: `by_borough` writes in 8.1s vs.
`by_month`'s 46.8s in *this specific run* (a difference driven mainly by Spark
plan/shuffle specifics of a skewed key, not a general property of "fewer
distinct values = faster write" — a controlled re-run holding shuffle
partitions fixed per partition count would be needed to isolate write cost
cleanly), while accumulating many more small files (207 vs. 30), which would
compound over repeated incremental writes (Week 3) into a small-files problem
that eventually needs periodic `OPTIMIZE`/compaction to control.

**Practical takeaway for this platform.** Neither strategy is universally
correct. `pickup_year, pickup_month` is the right choice for the platform's
system-of-record bronze/gold tables, because it matches how data actually
arrives (one file per month) and keeps partitions evenly sized — the property
that makes partitioning cheap to maintain under incremental updates. A
borough-partitioned *derived* table, rebuilt periodically and optimized for
borough-grouped analytical queries specifically (e.g. Week 2's per-borough
demand and fare queries), is a reasonable *additional* artifact to build for
serving that query pattern fast, exactly the way this benchmark constructed
one — but it should not replace the time-partitioned bronze/gold layer as the
platform's primary storage, because its skew (one partition holding 89.6% of
all data) undermines the maintainability benefits partitioning is supposed to
provide as data keeps growing.