# Documentation & Evidence

Start with the project overview in [English](../README.md) or
[简体中文](../README.zh-CN.md), then use the [runbook](RUNBOOK.md)
for commands or the [architecture](ARCHITECTURE.md) for platform behavior.
All runbook commands execute from the repository root.

## Assignment reports

| Assignment | Design | Evaluation | Submission reports |
| --- | --- | --- | --- |
| Week 1 — Data foundation | [Design](DESIGN_REPORT.md) · [Data catalog](DATA_CATALOG.md) | [Storage benchmark](BENCHMARK_REPORT.md) | Historical Markdown reports |
| Week 2 — Analytics & optimization | [Design](Lab2_Design_Report.md) | [Benchmark](Lab2_Benchmark_Report.md) | [Design PDF](../reports/Lab2_Design_Report.pdf) · [Benchmark PDF](../reports/Lab2_Benchmark_Report.pdf) |
| Week 3 — Incremental platform | [Design](Lab3_Design_Report.md) | [Evaluation](Lab3_Evaluation_Report.md) | [Design PDF](../reports/Lab3_Design_Report.pdf) · [Evaluation PDF](../reports/Lab3_Evaluation_Report.pdf) |
| Week 4 — Demand modelling | [Design](Week4_Design_Report.md) | [Evaluation](Week4_Evaluation_Report.md) | [Design DOCX](../reports/Week4_Design_Report.docx) · [Evaluation DOCX](../reports/Week4_Evaluation_Report.docx) |

## Read the implementation

| Concern | Entry points |
| --- | --- |
| Dataset contracts and transforms | [Dataset configuration](../config/datasets.yaml) · [Transforms](../src/urban_platform/transform/transforms.py) |
| Ingestion and quality checks | [Ingestion](../src/urban_platform/ingestion/) · [Integration](../src/urban_platform/integration/) |
| Analytics and performance | [SQL](../sql/) · [Analytics](../src/urban_platform/analytics/) · [Benchmarks](../src/urban_platform/benchmark/) |
| Incremental refresh and recovery | [Runner](../src/urban_platform/incremental/runner.py) · [Publication](../src/urban_platform/monitoring/publication.py) · [Recovery tests](../tests/test_week3_recovery.py) |
| Training data and models | [ML configuration](../config/ml.yaml) · [ML module](../src/urban_platform/ml/) · [Demand SQL](../sql/ml/zone_hour_demand.sql) |

## Evidence and reproducibility

- [Week 2 evidence](../evidence/): raw timings, before/after SQL, execution plans,
  query results, environment records and validation. Each experiment directory
  includes `measurements.json`; see the benchmark report for trial methodology.
- [Week 3 evidence](../artifacts/week3/): saved paired evaluations, publication,
  release and recovery checks, monitoring results and historical test output.
- Week 4 metrics are documented in the evaluation report. New runs write JSON
  under `artifacts/week4/` and models under `data/ml/`; these local outputs are
  ignored by Git and are not shipped as pretrained models.

The Week 1, Week 2 and strict Week 3 bootstrap runs use different validation
rules and therefore have different accepted-row counts. Keep each result with
its own baseline and environment; the reports are historical snapshots.
Recorded test outcomes describe their original runs, not a fresh test execution.

## Historical process notes

- [Earlier run guide](Week2_Run_Guide.md) — retained for context; the current
  [runbook](RUNBOOK.md) is the primary execution guide.
- [Week 2 implementation log](archive/LAB2_PROGRESS.md).
- [Week 3 audit and recovery verification](archive/Week3_Audit_Result.md).

Source code, SQL, configuration and report paths remain stable so existing
commands and report builders continue to work. Raw inputs, generated tables,
local runtimes and new submission ZIPs stay outside version control.
