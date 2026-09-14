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

## Pending user information

Weather provenance: weather.csv has measurement-source columns but no explicit
location, timezone, unit dictionary or weather-code legend. User was asked for
the course download/source documentation immediately. Do not claim NYC/UTC/unit
semantics are confirmed or run dependent full weather analyses until resolved.

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

## Pending source decision and production work

The user will supply the dataset; compare its checksums with the existing inputs
when it arrives. The user states that no additional source information should be
expected if absent from both the dataset and assignment. A follow-up question is
pending: may we retain Lab 1's UTC weather interpretation as an explicitly
documented assumption? `weather_interpretation_approved` remains false until the
user answers; `weather_provenance_status` must remain unverified if this is an
assumption rather than actual source evidence.

After that decision:

1. Ingest weather, regenerate Gold, and verify one row per accepted taxi trip.
2. Run all six queries and four products against full data, check coverage.
3. Run controlled optimization experiments and inspect plans for actual effects.
4. Generate the 3-5 page design report and benchmark report from real results.
5. Verify README reproduction, package final source/reports/evidence in ZIP.

No full-data Lab 2 weather result or final performance result has been measured
yet. Synthetic-test timings must never be presented as coursework benchmarks.

No remote push or course submission has been performed.
