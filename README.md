# ID2221 Urban Data Platform

This project combines NYC taxi trips, weather observations, air-quality measurements
and taxi-zone references using PySpark and Delta Lake. Week 3 adds incremental
updates, validation and quarantine, selective analytical refresh, and operational
monitoring. The instructions below reproduce the Week 3 workflow on Windows.

## 1. Environment and input data

Run all commands from the project root in PowerShell. The tested environment is
Python 3.12, Java 17, PySpark 4.0.4 and Delta Lake 4.0.1. The setup script installs
the project dependencies, a local JDK and the Windows Hadoop binaries. Downloads
require network access. Replace the two example paths with paths on your machine.

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup_windows.ps1 -PythonExe "D:\path\to\python.exe"
& .\.venv\Scripts\python.exe .\scripts\prepare_local_data.py "D:\path\to\course-files"
& .\.venv\Scripts\python.exe .\scripts\check_environment.py
```

The course-files directory must contain the three January-March 2024 taxi Parquet
files, `weather.csv`, `taxi_zone_lookup.csv`, and either `air_quality.zip` or its
extracted `air_quality/hourly_88101_2024.csv`. Preparation verifies the available
inputs against `config/input_manifest.json` and writes provenance information.
When only the extracted CSV is available, the ZIP checksum remains unverified.

The default Spark master is `local[4]`, with 4 GB of driver memory and eight shuffle
partitions. `LAB2_SPARK_MASTER` and `LAB2_DRIVER_MEMORY` override these settings.
Record overrides when comparing timings. Keep sufficient disk space for the raw
inputs, Delta history and evaluation workspaces; the raw air-quality CSV alone is
about 2.37 GB.

## 2. Generate the update release

```powershell
& .\.venv\Scripts\python.exe .\scripts\generate_week3_updates.py --source .\dataset --output .\data\updates --seed 2221
```

The command writes four update files and `data/updates/manifest.json`. The manifest
records counts, time ranges, schema additions and SHA-256 checksums. For the course
inputs, the release contains:

| Dataset | New rows | Modified rows | Exact duplicates | Schema addition |
| --- | ---: | ---: | ---: | --- |
| Taxi trips | 716,608 | 0 | 143,322 | None |
| Weather | 168 | 6 | 0 | `humidity` |
| Air quality | 504 | 3 | 0 | `aqi` |
| Taxi zones | 0 | 3 | 0 | None |

The taxi counts correspond to 7.5% new trips and 1.5% copied trips, using the
original 9,554,778 rows as the denominator. New timestamps follow each source's
own latest timestamp. The source datasets end on different dates; a few historical
corrections make the effect of environmental and zone changes observable in the
existing trip period. Invalid examples are kept in tests rather than the release.

## 3. Create a baseline and process updates

For a fresh project, initialise the strictly validated baseline once:

```powershell
& .\.venv\Scripts\python.exe .\scripts\run_week3_pipeline.py --bootstrap --run-id week3-baseline
& .\.venv\Scripts\python.exe .\scripts\run_week3_evaluation.py --capture-baseline
```

Bootstrap rebuilds the historical Bronze, Gold and product tables. Baseline capture
saves the state needed for controlled evaluation, so it must happen before applying
the update release. An existing evaluation baseline is protected against accidental
replacement; use `--replace-baseline` only when intentionally changing the experiment.

Apply the release to an existing baseline:

```powershell
& .\.venv\Scripts\python.exe .\scripts\run_week3_pipeline.py --updates-dir .\data\updates --run-id week3-release-1
```

Use a distinct run ID for each attempt. A successful run writes an execution summary
under `artifacts/week3/` and a version manifest in the Delta table
`data/monitoring/published_snapshots`. Readers use the latest successful manifest.
Bronze MERGE inserts new keys, ignores exact duplicates and updates corrections
for weather, air quality and zones. Taxi IDs are full-row hashes; a modified taxi
payload cannot be identified reliably as a correction without a source trip ID.

The same release can be submitted again with a new run ID to check idempotence.
Inspect the summary: inserts and updates should be zero, and unchanged analytical
tables should keep their versions. Change Data Feed (CDF) recovers all Bronze and
Gold changes since the last successful publication, including pending changes
when a new batch arrives. Products are recomputed for every affected intermediate
group, so an interrupted product refresh can be replayed. Retain the source files
and Delta/CDF history until recovery completes. This local runner assumes one writer.

## 4. Schema changes and validation

Rules are declared in `config/datasets.yaml`: required fields, numeric ranges,
timestamps, foreign keys, primary keys and permitted additive source columns.
Weather accepts `humidity`, transformed to `humidity_pct_v2`; the original
`relative_humidity_pct` remains available. Air quality accepts `aqi`, exposed in
Gold as `aqi_max`. CSV source columns are read as strings and converted to numeric
types before value validation.

To allow a new field, add its raw type to `allowed_raw_additions`, update the dataset
transform and add any necessary validation rules. Update affected product definitions
and tests when the field changes their meaning. Renames, removals, incompatible types
and new business rules need an explicit migration. The platform does not infer one.

Invalid rows are stored in per-run Delta quarantine tables with rejection reasons;
the run summary gives `quarantine_path` and `validation_failures`. Valid rows can
continue through row-level validation. An unsupported schema fails that dataset and
blocks the new analytical publication. Duplicate counts and rejected-invalid-row
counts are separate; one invalid row may also violate several rules.

## 5. Monitor and validate the published state

```powershell
& .\.venv\Scripts\python.exe .\scripts\run_week3_monitoring.py
& .\.venv\Scripts\python.exe .\scripts\validate_week3_platform.py
& .\.venv\Scripts\python.exe -m pytest tests -q --junitxml=artifacts/week3/pytest-all.xml
```

Monitoring reads `data/monitoring/pipeline_runs` and runs the four queries in
`sql/monitoring/`. Results appear in `artifacts/week3/monitoring/`: validation
frequency, slowest datasets, rejected records per execution and processing-time
trends. Exact duplicates count as validation events, so interpret the failure-frequency
report together with rejection counts and run status.

The platform validator writes `artifacts/week3/final_validation.json`. It checks
update-file hashes, the published Gold/taxi row counts, access to all four products,
execution of the six Week 2 queries and incomplete-month handling. It is a final
consistency check; full-result comparison is performed by the evaluation below.

## 6. Reproduce the evaluation

```powershell
& .\.venv\Scripts\python.exe .\scripts\run_week3_evaluation.py --trials 2
```

Each trial starts three workspaces from the same saved baseline: incremental refresh,
full analytical rebuild, and incremental refresh with event monitoring disabled.
All three run the same ingestion, orchestration and publication code. Variant order
reverses between trials. Gold fingerprints, all four products and all six queries
are compared for both the refresh and monitoring pairs. The production command
always keeps monitoring enabled.

Validation uses paired controls over the same fully materialised transformed rows.
Both arms consume every original column and row; the enabled arm additionally
checks schema and values, then recombines accepted and rejected rows for the same
checksum sink. Preparation is timed separately. Storage inventories count every
file under each trial's `data/`, separating active analytical data, retained files,
CDF, logs, quarantine, staging and metadata. No VACUUM runs during measurement;
hard links count as logical file bytes, not additional physical disk allocation.

Results go to `artifacts/week3/evaluation.json`. After an interruption, `--resume`
reuses completed trials only if the code, configuration and update manifest still
match their saved signature. Raw paired differences, including negative values
caused by timing noise, are retained. Two local trials remain a limited sample.

## 7. Reports and submission

```powershell
& .\.venv\Scripts\python.exe .\scripts\build_week3_reports.py
& .\.venv\Scripts\python.exe .\scripts\package_submission.py
```

Report generation uses the saved evaluation, baseline and release records. It writes
the evaluation Markdown and both PDFs; the design Markdown is maintained by hand.
Review the PDF pages after changing content. Packaging writes
`../output/Week3_Submission.zip` and verifies the archive checksums. Original course
inputs and local runtimes are excluded. The archive places generated update files in
`updates/`; copy them to `data/updates/` after extraction to use the commands above,
or regenerate them with the same seed.

| Location | Contents |
| --- | --- |
| `src/urban_platform/` | Ingestion, integration, incremental refresh and monitoring |
| `scripts/`, `config/` | Command-line entry points and dataset/analysis configuration |
| `sql/` | Six analyses, four products, optimized queries and monitoring SQL |
| `tests/` | Small-data correctness, validation and refresh tests |
| `docs/Lab3_Design_Report.md` | Design, decisions and limitations |
| `docs/Lab3_Evaluation_Report.md` | Measured results and interpretation |
| `reports/` | Submission PDFs |
| `artifacts/week3/` | Local run and evaluation evidence |
| `evidence/` | Historical Week 2 experiment snapshot |

Week 3 extends the analysis window through the latest valid taxi day; the original
Week 2 end date remains separately recorded. Storage uses UTC and reporting uses
New York time. The weather source does not state its timezone, so treating it as
UTC remains an assumption that may shift associations by four or five hours.
Air-quality correlations describe association rather than causation.

Earlier setup and experiment notes are retained in `docs/Week2_Run_Guide.md` as a
historical reference. Use the workflow above for Week 3.
