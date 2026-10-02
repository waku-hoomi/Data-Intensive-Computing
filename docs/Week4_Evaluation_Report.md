# Week 4 Evaluation Report

ID2221 Data-Intensive Computing | October 2026

## 1. Feature engineering analysis

The assembled feature vector has 9 dimensions: 3 indexed categoricals
(`pu_location_id`, `pickup_borough`, `weather_condition_code`), 4 derived
temporal features (`hour_of_day`, `day_of_week`, `month`, `is_weekend`), and
5 imputed numeric weather/air-quality columns (`temp_c`,
`relative_humidity_pct`, `precipitation_mm`, `wind_speed_kmh`, `pm25_ug_m3`).

Two Gold columns that exist in the schema, `humidity_pct_v2` and `aqi_max`,
were excluded from the feature list after the first training attempt failed
with `surrogate cannot be computed`: on the bootstrapped baseline (no Week 3
update release applied) both columns are **100% NULL** --
0 of 571,613 rows -- because the platform only ever populates them from an
applied update release. This is a different and more severe condition than
`pm25_ug_m3`'s genuine coverage gap (372,538 of 571,613 rows non-null, 65.2%):
air quality is honestly partially missing for a structural reason (no PM2.5
station in Manhattan or Staten Island); the two excluded columns are entirely
unavailable under this run's data state. A median `Imputer` can do useful work
on the former and nothing at all on the latter.

Among the included columns, `pm25_ug_m3` required the most preprocessing
thought, not the most code: the decision was to impute rather than drop,
because dropping the 34.8% of rows with no air-quality reading would remove
Manhattan and Staten Island from the training set almost entirely, concentrating
the model's signal on three boroughs and silently making it unusable for two.
The temporal features required the least -- a single `SQLTransformer` call --
but contribute real signal: taxi demand is strongly periodic by hour and by
weekday, and a model without these columns would have to approximate that
periodicity through weather alone, which it should not be expected to do well.

## 2. Model evaluation

Trained a `GBTRegressor` (maxIter=40, maxDepth=6, seed=2221) inside the full
feature `Pipeline`, on the chronological train split (Jan 1 -- Feb 29,
371,665 rows), evaluated on the validation split (Mar 1--20, 124,320 rows) and
the held-out test split (Mar 21--31, 75,628 rows). Fitting took 56.2 s on
local[4].

| Split | RMSE | MAE | R^2 |
| --- | ---: | ---: | ---: |
| Validation | 34.42 | 13.15 | 0.614 |
| Test | 33.58 | 12.83 | 0.580 |

58.7% of all zone-hours in the dataset have zero demand, so a share of this
accuracy comes from correctly predicting near-zero for low-traffic zone-hours;
the R^2 of 0.58 on the held-out, chronologically later test split indicates the
model captures real structure beyond that base rate, but we did not decompose
error by zone-traffic tier and do not claim the model performs as well on
high-volume zones specifically.

<!-- PAGEBREAK -->

## 3. Approach A vs. Approach B: what re-deriving the dataset costs

Both approaches ran the identical feature pipeline and `GBTRegressor`
configuration; only the dataset-construction step differed. Wall-clock dataset
construction was fast for both (7.3 s for Approach A, 15.0 s for Approach B --
Approach B pays more here because it also re-validates its session's publication
pointer), so timing is not the interesting comparison on this single-machine
scale. The datasets themselves are not the same table, in ways that would not
be obvious without comparing them directly:

| | Approach A (raw) | Approach B (platform) |
| --- | ---: | ---: |
| Zone-hour rows | 580,350 | 571,613 |
| Distinct zones | 265 | 259 |
| Distinct hours | 2,190 | 2,207 |
| Total demand (sum of trip_count) | 9,554,778 | 9,323,674 |
| Test R^2 / RMSE | 0.623 / 33.48 | 0.580 / 33.58 |

**Approach A's total demand equals the raw, unvalidated trip count exactly.**
Bronze's validation rejects 231,088 of the 9,554,778 raw trips in this dataset
(duplicate keys, invalid fare amounts, negative durations, foreign keys to
zones that do not exist) before Gold is ever built. Approach A, which reads the
parquet files directly and never calls any validation, counts every one of
those 231,088 rejected trips as real demand -- a 2.4% label inflation with no
visible symptom; the resulting numbers look entirely plausible. Approach B's
total (9,323,674) is 15 rows below Bronze's accepted count of 9,323,689, which
we attribute to the `[start_date, end_date_exclusive)` analysis-window filter
in `analysis_trips` clipping a handful of trips at the boundary; we did not
trace the exact rows, since the discrepancy is 0.0002% of the total and does
not change any conclusion here.

**Approach A's raw data contains a timestamp defect the platform's own
validation range does not catch either, but its analysis-window filter does.**
Approach A's zone-hour grid spans pickup hours from `2003-01-01 11:00` to
`2024-04-01 12:00` -- one raw trip carries a pickup timestamp 21 years before
the study period, a known class of defect in the public NYC TLC parquet files
(meter clock error). Week 1-3's Bronze validation rule for taxi timestamps
("outside 2000-2100") is deliberately generous and would not reject this row
either. What excludes it from Approach B is a different, analysis-layer
control: `analysis_trips` (reused from `analytics/context.py`, not
re-derived) filters to the configured `[2024-01-01, end_date_exclusive)`
window. Approach A, built directly from raw files, has no equivalent filter --
reusing the platform bought this exclusion for free; re-deriving the dataset
from scratch would have required remembering to add it.

**The two approaches do not even agree on which zones exist in the grid.**
Approach A enumerates all 265 rows of the raw zone lookup; Approach B
enumerates only the 259 zones that appear as a validated pickup location.
The six zones present only in Approach A are location IDs 99, 103, 104, 110,
264 and 265. IDs 264 and 265 are exactly the two rows Bronze's
`taxi_zone_lookup` validation rejects (`Zone: "N/A"` and `Borough: "N/A"`
respectively -- the platform's text-normalisation treats the literal string
`"N/A"` as missing, so these fail the `required_fields` check); any trip
referencing them as a pickup location is in turn rejected for a broken foreign
key, so Approach B correctly has no basis to include them. IDs 99
(Freshkills Park), 110 (Great Kills Park) and 103/104 (Governor's
Island/Ellis Island/Liberty Island, which the raw lookup table lists under two
separate location IDs for the one zone name) are legitimate zones with zero
recorded pickups anywhere in the three-month window -- consistent with their
being park and island areas with little or no taxi infrastructure, though we
did not independently verify that explanation beyond the observed zero count.

**Despite all of this, the two models land in a similar place**
(test R^2 0.623 vs. 0.580, RMSE 33.48 vs. 33.58): gradient-boosted trees
proved fairly robust to this amount of label noise and grid disagreement on
this dataset. That robustness is the risk, not the reassurance -- Approach A's
model trained and evaluated without any visible error, on a target that was
silently wrong for 2.4% of its mass and a grid that disagreed with the
validated one on which zones exist at all. A team relying on Approach A would
have no signal, from the pipeline itself, that anything was off.

## 4. Reproducibility

The feature pipeline and model are saved as a single `PipelineModel`
(`data/ml/models/<run_id>/`), so loading and scoring new data does not depend
on separately re-running the feature code in the right order. The dataset
build is deterministic given the Gold table's published version: the same
`run_id`-free call to `build_training_dataset` against an unchanged Gold
version returns the same rows via Delta's version pinning
(`read_integrated`), and the chronological split boundaries come from
`config/ml.yaml`, not from the data itself, so they do not drift between runs.
What is not yet reproducible across machines is the Approach A/B timing
comparison in Section 3 -- both numbers were measured on one Windows laptop
under local[4] with no repeated trials, so they should be read as an existence
proof that the two paths differ, not as precise relative costs.

## 5. The role of data engineering

Weeks 1-3's validation, integration and analysis-view layer did three
specific things for this ML task that re-deriving the dataset from raw files
did not get automatically: it excluded 231,088 invalid trips from the label
before the model ever saw them; it clipped the time range to the intended
study window, incidentally also discarding a 21-years-off timestamp defect
that a numeric range check alone does not catch; and it excluded two
placeholder zone codes (`"N/A"`, `"Outside of NYC"`) whose text values the
platform's own normalisation had already flagged as missing. None of these are
machine-learning-specific decisions -- they are the same validation and
integration rules Weeks 1-3 built for entirely different (SQL analytics)
purposes, and Task 1's dataset query inherited them by calling
`register_analysis_views`/`read_integrated` rather than reading parquet and
CSV files directly.

The features that contributed the most distinct signal were the ones that
required joining across datasets rather than transforming one in isolation:
weather and air-quality conditions at the pickup hour, which only exist in the
feature table because Week 1's integration pipeline had already resolved the
timezone mismatch between NYC-local taxi timestamps and UTC weather/air-quality
readings. Re-deriving that join correctly (Approach A did, for this report,
but by duplicating the same timezone and county-to-borough logic a second
time) is exactly the kind of work a second, parallel "ML ingestion" path would
have to keep in sync with the original by hand.

If a new dataset were introduced (for example, event or holiday calendars),
the reusable part of this pipeline is the `features.py` column lists and the
`SQLTransformer` statement -- the places designed to grow by adding a name, not
by changing a stage's shape. The platform-side work would still be the larger
share: the new source would need its own validation rules and integration join
before Task 1's SQL query could reference it, exactly the pattern Weeks 1-3
already established for weather and air quality. The improvement to the
platform that would most help future ML work here is a narrower one than a new
dataset: an explicit flag on `read_integrated`/`register_analysis_views` for
"was this Gold table built from a bootstrap or does it include an applied
update release," so a training run can assert its own precondition instead of
discovering a structurally-empty column the way this one did.
