# Lab 2 implementation status

Deadline: 2026-09-16 (exact time not provided). Plan approved 2026-09-14.
Base commit: 5187bfa44aec7a5fa188885803f6c7f7bc2d2cce. Local branch: codex/lab2.

## Confirmed design choices

- Build on the group's Spark 4.0.4 / Delta 4.0.1 project.
- Correct NYC air-quality geography and station weighting before Lab 2.
- Use Spark SQL for all six analytical queries.
- Store UTC timestamps; report demand by New York local date/time and pickup location.
- Scope analysis to January-March 2024, using an explicit half-open local-time interval.
- Create four Delta data products with metadata.
- Investigate caching, partition pruning, broadcast joins and AQE independently.
- Verify result equality; record warm-up separately and retain three timed runs.
- Deliver source, 3-5 page design report, benchmark report, README and ZIP.

## Approved weather interpretation

On 2026-09-15 the user explicitly approved continuing Lab 1's UTC weather
interpretation and declined sensitivity analysis. The source timezone remains
unverified. This assumption is recorded in configuration, both reports and the
Lab 3 handoff; no further user decision is pending for this scope.

## Completed and verified on 2026-09-14

- Latest remote HEAD verified; isolated clone and branch created.
- Exact runtime installed: Python 3.12.14, Spark 4.0.4, Delta 4.0.1,
  Java 17.0.20.1, local[4]. Real Delta round-trip passed.
- Six existing course inputs passed the stored SHA-256 manifest checks.
- NY state/county filtering and equal station weighting implemented.
- Input column checks expanded; malformed numeric/date parsing is ANSI-safe;
  generated trip IDs preserve NULL positions and field names; valid duplicates
  are preferred deterministically; negative durations are rejected.
- Full independent ingestion: zones 265 -> 265; air quality 8,139,551 ->
  7,928,224; taxi trips 9,554,778 -> 9,417,864. There are 180 negative-duration
  rejects in addition to the previous checks. Actual logs are in data/_metadata/.
- 9,417,864 unique trip IDs verified, with no negative durations remaining.
- Candidate vendor/time/location composite key has 348 duplicate groups in
  cleaned data, supporting use of the deterministic generated ID.
- Geographic invariance passed on full air-quality data: removing every
  out-of-state observation cannot change NYC borough-hour summaries.
- Six SQL analyses, four Delta product builders, metadata registry, optimized
  queries and the controlled experiment runner are implemented.
- Nine tests passed, including manually computed query examples, zero-demand
  hours, DST, actual Delta product refresh and metadata, optimized equality,
  zero-variance correlation, and the experiment runner's trial/plan retention.

## Supplied dataset verification

The user supplied `/Users/felixchen/Downloads/data/` on 2026-09-14. All six files
passed SHA-256 comparison against the existing course input manifest; no data
replacement or repeated ingestion is needed. The directory contains only the six
data files, and the air-quality ZIP contains only its CSV. No additional weather
location, timezone, unit dictionary or code legend was supplied.
The user states that no additional source information should be expected if
absent from both the dataset and assignment. The approved interpretation does
not change `weather_provenance_status: unverified` into a verified source claim.

## Completed full-data work on 2026-09-15

1. Ingested all 8,784 weather observations and regenerated Gold with 9,417,864
   unique trip IDs; 9,417,844 trips lie in the declared local Jan-Mar interval.
2. Executed all six SQL queries and generated four Delta products from Gold
   version 0. Product totals match their declared source scopes.
3. Completed all ten original/optimized comparison pairs: four independent
   techniques and six final queries, with 60 measured executions. Every pair
   passed result and schema equality checks. All physical plans are retained.
4. Verified in-memory cache scans, partition filters, broadcast hash join and
   final AQE coalesced shuffle in actual execution plans.
5. Generated and visually inspected a 5-page design PDF and 3-page benchmark PDF,
   plus editable Markdown. Reports use actual measurements, including setup
   costs, source limitations and the approved weather assumption.
6. Final tests: nine passed. Full validation, input manifest, per-run metrics,
   query outputs, plans and test results are included in the submission evidence.

Synthetic-test timings are separate from the full-data benchmark evidence.

No remote push or course submission has been performed.
