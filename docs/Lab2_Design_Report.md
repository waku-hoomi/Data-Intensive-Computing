# Design Report

Urban Analytics Platform

Lab 2 Design Report | ID2221 Data-Intensive Computing | 15 September 2026

## 1. Analytical requirements and data foundation

This extension builds reusable Spark SQL analyses and four materialized Delta products on the group's Lab 1 platform. It answers six mobility/environment questions, investigates caching, partition pruning, broadcast joins and Adaptive Query Execution (AQE), and verifies that optimization preserves results. Entry points and SQL are separate so analysts can repeat a query or refresh products without editing pipeline internals.

The six checksum-verified course inputs contain three Yellow Taxi Parquet files for January-March 2024, weather CSV, EPA PM2.5 CSV and the taxi-zone lookup. Corrected ingestion retains 9,417,864 unique trips, 7,928,224 air-quality measurements, 8,784 weather observations and 265 zones. Full Gold has exactly one row per accepted trip. Analysis uses 9,417,844 trips within local 2024-01-01 inclusive to 2024-04-01 exclusive; 20 accepted trips fall outside this explicit interval.

## Corrections inherited by every analysis

NYC air quality is selected with New York state code 36 and NYC county codes before aggregation. California/Kings must never become Brooklyn. Multiple instruments are averaged within a station-hour, then distinct stations receive equal weight within a borough-hour. Full-data geographic invariance was checked by removing all out-of-state input: NYC aggregates did not change.

Taxi IDs hash sorted named JSON fields, preserving NULL positions. A candidate vendor/time/location key still has 348 duplicate groups in cleaned data, so it is not a reliable key. Schema checks now cover columns used by transformations; invalid parses tolerate ANSI mode; deterministic duplicate handling prefers valid rows. An additional 180 negative-duration trips were rejected. These changes require regenerated Lab 1 tables; old and new ID schemes must not be mixed.

## Time and missing-data policy

Storage timestamps remain UTC; pickup demand is reported in America/New_York local time. The course weather CSV does not specify its timezone. We retain Lab 1's UTC interpretation as an explicitly approved but unverified assumption. No sensitivity experiment was performed. If its timestamps actually represent NYC local time, weather associations may shift by 4-5 hours. Numerical weather codes remain categories without invented labels. Missing environmental context remains NULL, not zero.

Scope and limitation: analysis describes accepted Yellow Taxi records in one quarter, not every NYC journey. Association is not causation. Source provenance and cleaning decisions constrain interpretation even when SQL and optimization checks pass.


---

## 2. Reusable Spark SQL query design

All six analyses live in sql/queries/. The common analysis_trips view applies the configured UTC bounds corresponding to the local reporting interval and exposes local date, month, pickup hour and duration. SQL views organize shared calculations; the source and products remain Delta tables.

| Query | Definition and output |
| --- | --- |
| Q1 Zone monthly demand | Count accepted pickups by local month, pickup-zone ID, zone and borough. Retain unknown geography as a separate NULL group. |
| Q2 Weather and distance | Group by source weather code. Return trip count, valid distance count and average trip distance in TLC miles. |
| Q3 PM2.5 and demand | One pair per borough-hour with available PM2.5: measurement, trip count. Return sample size, mean PM2.5, mean demand and Pearson correlation. |
| Q4 Weather demand variation | For each zone/code calculate trips per observed hour. Require 24 hours per code and two eligible codes per zone; rank max-minus-min hourly rates. |
| Q5 Weekly peak hours | Group demand by ISO weekday and local hour; divide by actual observed elapsed hours. Preserve tied peak hours with DENSE_RANK. |
| Q6 Monthly trend | Report monthly trips, calendar days, daily mean and month-over-month percentage. The first month has no prior-quarter comparison. |

## Correct denominators and comparison units

An explicit UTC hour spine contains 2,183 elapsed hours in the 91-day local interval, including the March DST transition. Crossing observed zone IDs with these hours retains zero-trip zone-hours. This prevents rare weather from appearing to lower demand solely because it occurred for fewer hours. Q4 compares category-specific means; it is not a causal model and does not adjust for weekday, season or time of day.

Q3 avoids trip-weighting a repeated hourly PM2.5 value. Safe covariance divided by the product of standard deviations returns NULL when variance is zero. Borough-hours without inherited air-quality context are excluded from correlation and remain flagged missing in the product. Because context comes from the integrated trips, a zero-trip borough-hour cannot recover a sensor observation from that table alone; the reported association is conditional on available context.

Q5 uses actual hourly observations so an absent spring-forward hour is not counted as an observed zero. Q6 uses calendar days, not only days on which a trip was observed. The same definitions are used before and after optimization.


---

## 3. Materialized analytical data products

The four products are generated automatically from a version-pinned integrated Delta table. All are small enough to remain unpartitioned in this run. Materialization moves repeated aggregation to refresh time, while keeping storage and freshness costs explicit.

| Product / grain | Consumer and materialization rationale |
| --- | --- |
| Daily mobility / local date + borough | Operations and planning analysts inspect daily volumes and monthly trends. Store counts plus sums and valid counts for distance, duration and fare, enabling correctly weighted roll-ups. |
| Taxi-zone statistics / month + zone | Transport planners compare zones without scanning individual journeys. Store counts and mean distance, duration and fare; Q1 reads the prepared monthly counts. |
| Weather impact / zone + weather code | Service planners compare rates and trip distances by observed condition code. Store observed hours, trips, distance sums/counts and mean hourly demand, reusing the costly zero-hour scaffolding. |
| Air-quality impact / borough + hour | Environmental analysts reuse demand, PM2.5, station count and availability flags. One stored observation per borough-hour supports Q3 without repeatedly regrouping trips. |

## Metadata and refresh contract

Each product is a Delta table under data/products/. An append-only Delta registry under data/lab2_metadata/product_registry records product name, source path and Delta version, initial creation time, refresh time, schema version and schema JSON, analysis configuration, row count, active-file count, storage bytes and build duration. Refresh overwrites a product and appends a registry record; initial creation time is retained.

Products in this experiment use Gold version 0. The benchmark refuses product metadata referring to another Gold version or analysis configuration. Refreshes run serially in this local batch workflow. Publication across the product table and registry is not a multi-table transaction; simultaneous refresh/benchmark execution is outside the supported workflow. A production service should publish a manifest only after all tables are ready.

The four products total 244,997 active Parquet bytes (0.234 MiB), excluding Delta logs. Their counts and sums were validated against source scope. The air product covers 9,413,067 trips with a non-null borough; 4,777 trips with missing borough cannot receive a borough-hour group. Daily, zone and weather products preserve the full scoped trip count.

Creation/refresh timestamps in metadata are ISO-8601 UTC strings. Large raw inputs and generated Delta tables are reproducible outputs and are not required inside the source submission ZIP.


---

## 4. Optimization strategy and evidence

Each technique is investigated independently with result equality checks. A fixed baseline disables AQE and automatic broadcast joins, retaining eight shuffle partitions. This is an explicit experimental baseline, not a claim that Spark's defaults always behave this way. All results are local warm-process measurements; file-system caches are not flushed.

| Technique | Selected experiment / expected plan evidence |
| --- | --- |
| Caching | Q2 on analysis_trips, uncached versus explicitly materialized SQL cache. Expect an in-memory scan. Cache setup is measured separately and must be amortized. |
| Partition pruning | Q1 restricted to February in both variants. Add pickup_year=2024 and pickup_month IN (2,3). UTC March is required for February's final local evening. Expect PartitionFilters. |
| Broadcast join | Read underlying Bronze taxi trips and join the 265-row zone lookup. Compare MERGE with BROADCAST(z), holding scope fixed. Expect BroadcastHashJoin BuildRight. |
| AQE | Run Q1 with identical SQL, eight initial shuffle partitions and broadcasting disabled; switch AQE only. Check final adaptive plan and coalesced shuffle readers. |

Final query comparisons use materialized zone statistics for Q1, explicit analysis cache for Q2, the hourly air product for Q3, the weather summary for Q4, cached city-hours for Q5, and daily mobility roll-ups for Q6. Each retains the baseline result schema and semantics. No technique is claimed to apply usefully to every query.

Trade-offs differ by technique. Explicit partition predicates couple a query to the physical layout and require careful local/UTC boundaries. Broadcast replicates the small side in executor memory, so it must be bounded by measured table size. AQE adds runtime planning work and can change parallelism between executions. Cache consumes memory or spills to disk, has initialization cost, and must be invalidated or refreshed when inputs change.

## Measurement discipline

Each variant has one warm-up immediately before each of three timed collect() executions. Pair order alternates before/after, after/before, before/after. Spark caches are cleared between variants; cache setup is outside query latency but retained separately. EXPLAIN FORMATTED and the executed plan after collection are saved, including the final AQE plan. The three timings, result rows and equality outcome remain available in evidence.

Comparison ignores output ordering, requires exact integer/null agreement and uses relative tolerance 1e-9 plus absolute tolerance 1e-8 for floating-point aggregates. Both repeated executions and original/optimized outputs are checked. Nine automated tests cover manually calculated SQL, zero-demand hours, DST, zero variance, corrected geography, identity, schema parsing, Delta refresh and benchmark artifacts.

The benchmark report separates speedups among the four required techniques from the larger gains achieved by precomputing products; it also reports product build time, storage and cache setup cost.


---

## 5. Findings, limitations and engineering trade-offs

Demand increases from 2,927,082 trips in January to 3,523,934 in March. Calendar-normalized daily means rise from 94,422 to 113,675. Weekday peaks are 18:00; Saturday peaks at 19:00 and Sunday at 00:00 under the configured local-time definition. These describe this quarter of cleaned Yellow Taxi data.

PM2.5-demand Pearson correlations are 0.032 for Brooklyn, -0.051 for Queens and -0.030 for Bronx. They are weak unadjusted linear associations, not evidence that pollution changes demand. Q4 ranks Upper East Side North first, with an hourly-rate range of 204.83 trips across eligible weather codes. Time-of-day and season confounding remain.

Air quality is missing for roughly 90% of scoped trips, largely reflecting uneven geographic coverage. Weather matches every scoped trip under the UTC assumption, but a high matching rate cannot validate the timezone. Weather category meanings, precise station provenance and timezone were not supplied; descriptive weather labels and causal conclusions are deliberately avoided.

## Which trade-offs are justified here?

Small unpartitioned summaries provide strong repeated-query savings with very little storage overhead. Cached full analysis data is fast to reuse but expensive to initialize, so it should serve repeated workloads rather than a single one-off query. Borough-hour or zone-weather aggregation remains the heavier baseline work because it must scan/group trips and account for absent observations. Broadcast is appropriate for the tiny lookup, not the nationwide air-quality fact table. With only eight initial shuffle partitions, AQE has little excess parallelism to remove.

## Expansion to ten cities and handoff

Add city_id to every entity, key and aggregation grain; maintain per-city timezone and provenance configuration instead of extending NYC-specific assumptions. Monitor skew, file sizes and join-side size before selecting city/date partitioning or broadcast. Replace unconditional overwrite with source-version-aware incremental maintenance where justified; publish product versions and freshness metadata together. Avoid broadcasting an environmental table merely because it was small for one city.

Lab 3 should start from the regenerated Lab 2 baseline and keep the documented UTC weather interpretation. No alternative production dataset exists. Preserve weighted sums/counts when extending roll-ups, refresh products when Gold or analysis scope changes, and rerun key/row-count checks after ingestion changes. The current workflow is batch refresh, not an already implemented incremental platform.

Reproducibility: README, config/input_manifest.json, SQL files, source and tests accompany the reports. Final validation, source/product metadata, query outputs, all timing trials and physical plans are included as evidence. See docs/LAB3_HANDOFF_ZH.md for the concise group handoff.
