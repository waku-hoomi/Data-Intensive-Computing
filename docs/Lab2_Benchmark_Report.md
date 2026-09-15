# Benchmark Report

Query Performance Evaluation

Lab 2 Benchmark Report | Actual measurements: 15 September 2026

## 1. Environment and methodology

Single Apple M5 machine, 10 logical CPUs, 16 GiB RAM, local disk; macOS-26.6.2-arm64-arm-64bit. Python 3.12.14, Java 17.0.20.1, Spark 4.0.4, Delta 4.0.1; local[4], requested 4 GB driver, eight SQL shuffle partitions and UTC Spark session. Local date interpretation is America/New_York. Exact storage device model was not collected.

Gold version 0 has 9,417,864 rows; the Jan-Mar local interval contains 9,417,844. Baseline AQE and automatic broadcast are disabled to isolate changes. Each variant is warmed once immediately before each of three measured collect() actions; pair order alternates. Timings exclude SQL DataFrame construction, explicit cache setup and product creation, but include execution and result collection. Spark caches are cleared between variants; OS caches are not flushed.

All ten comparison pairs passed result and schema checks, using exact integer/null comparisons and 1e-9 relative / 1e-8 absolute floating tolerance. Outputs and plans are saved. These are repeated local measurements, not confidence intervals or evidence about a distributed cluster.

## 2. Effects of the four required techniques

| Experiment | Before (s) | After (s) | Speedup |
| --- | --- | --- | --- |
| Cache (Q2) | 0.2450 | 0.1049 | 2.34x |
| Pruning (February Q1) | 0.6964 | 0.5750 | 1.21x |
| Broadcast zone join | 0.9799 | 0.3082 | 3.18x |
| AQE (Q1) | 0.7212 | 0.6924 | 1.04x |

Broadcast has the largest improvement among these four experiments (3.18x). The small zone lookup replaces a shuffled sort-merge join with BroadcastHashJoin BuildRight. Pruning shows explicit year/month PartitionFilters; both queries cover identical February local-time records. The optimized predicate includes UTC months 2 and 3 to avoid dropping the final local evening.

Cache reads an InMemoryRelation instead of repeatedly scanning the Delta source, but its median population/setup time is 9.214 s in the technique experiment. AQE has only a modest 1.04x median change: the final plan shows AQEShuffleRead coalesced, with little room for improvement at this small initial partition count. Do not extrapolate this small difference to larger workloads.

All table values are medians of three timed runs. Each experiment's raw timings and before/after SQL, EXPLAIN FORMATTED and post-execution plan are under evidence/benchmarks/ in the submission package.


---

## 3. Final original/optimized comparison for every query

| Query / optimization | Before (s) | After (s) | Speedup |
| --- | --- | --- | --- |
| Q1: Zone product | 0.6871 | 0.0650 | 10.57x |
| Q2: Cached trips | 0.1980 | 0.0895 | 2.21x |
| Q3: Air product | 0.8388 | 0.0856 | 9.80x |
| Q4: Weather product | 1.7122 | 0.0903 | 18.96x |
| Q5: Cached city hours | 0.4745 | 0.0451 | 10.52x |
| Q6: Daily product | 0.8804 | 0.1037 | 8.49x |

Q1, Q3, Q4 and Q6 replace repeated full-data aggregation with equivalent queries on version-matched products. Q2 uses an explicit analysis cache; Q5 caches the city-hour intermediate. Their physical plans show small product scans or in-memory scans. Equality is checked for all output rows, not only counts or selected examples.

## 4. Materialization storage and creation cost

| Product | Rows | KiB | Build (s) |
| --- | --- | --- | --- |
| daily mobility summary | 717 | 30.28 | 4.451 |
| taxi zone statistics | 773 | 30.45 | 2.324 |
| weather impact summary | 3,668 | 73.21 | 3.294 |
| air quality impact summary | 15,281 | 105.32 | 1.537 |

Products total 244,997 active Parquet bytes (0.234 MiB), across four active data files. This is 0.0284% of Gold's 823.20 MiB active data. Delta transaction logs and the metadata registry are excluded from both data-size figures. Product table writes took 11.607 s in total; validation and registry writes are outside this build timer.

Q2's median cache setup is 10.065 s, compared with a 0.108 s steady-state saving. Under unchanged conditions, simple setup/saving amortization needs about 93 repeated queries before setup is recovered. This estimate ignores eviction and competing workloads. Q5's smaller city-hour cache takes 0.522 s to set up. A fast cached query is therefore not automatically a faster one-off workflow.

The products are regenerated from a pinned Gold snapshot. Registry metadata includes source version, configuration and schema, enabling stale products to be rejected before benchmarking. Source totals and product totals agree within each declared scope; the air product excludes trips lacking a borough instead of assigning a fabricated geography.


---

## 5. Variability, interpretation and limits

| Experiment | Before range (s) | After range (s) |
| --- | --- | --- |
| Cache (Q2) | 0.2255 - 0.5320 | 0.1010 - 0.1119 |
| Pruning (February Q1) | 0.6889 - 0.7341 | 0.5565 - 0.5784 |
| Broadcast zone join | 0.7941 - 1.0342 | 0.2935 - 0.3394 |
| AQE (Q1) | 0.7199 - 0.7302 | 0.6772 - 0.6948 |

Only three trials per variant were collected. The first cache baseline is slower than subsequent baseline trials, despite warm-up, illustrating residual JVM, scheduling and filesystem-cache effects. Median reporting and alternating order reduce sensitivity but do not eliminate it. Timings for the same Q2 SQL in its isolated technique experiment and later final comparison therefore differ; they are separate measurements, not interchangeable estimates.

Q4 is the most expensive original analytical query (1.712 s median): it builds zero-trip zone-hour combinations and groups demand by weather before ranking variation. Its weather product gives the largest final-query ratio (18.96x), but that shifts work to refresh time rather than removing it. Q3 and Q6 also repeatedly aggregate the large fact table; reusable hourly/daily summaries avoid that scan and shuffle for repeated access.

Data shape explains the results: 9.4 million trips join a 265-row lookup, so broadcast removes disproportionate shuffle work; monthly storage allows a bounded local-month query to omit irrelevant UTC partitions; small pre-aggregated outputs are cheap to read. AQE has limited impact with eight initial shuffle partitions. Caching adds memory pressure and significant preparation work; materialization adds freshness and maintenance obligations.

## Correctness and source limitations

The clean Gold table retains one unique ID per trip and no negative durations. All product totals were checked. Nevertheless, roughly 90% of trips lack air-quality context, and the UTC weather interpretation remains an unverified source assumption. Optimized equality proves that an optimization preserved the declared analysis, not that environmental provenance or causal interpretation is established. The source files were supplied without a weather timezone; no sensitivity experiment was performed.

## Recommendations for ten cities

Introduce city-aware keys and timezone configuration, profile skew and table sizes before selecting broadcast/partition strategies, compact small files and bound cache use. Publish source-version-aware product refreshes and monitor freshness. Test on an appropriate cluster with repeated workloads and representative concurrency before using these local ratios for capacity planning.

Evidence inventory: benchmark_all.json contains all 60 timed executions; products.json records row/file/size/build metrics; benchmark_environment.json records hardware/software; final_validation.json records scope and product totals; tests.xml records the automated test outcome. README gives commands to reproduce every step.
