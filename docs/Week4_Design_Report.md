# Week 4 Design Report: ML Pipeline on the Urban Data Platform

ID2221 Data-Intensive Computing | October 2026

## 1. Problem and training-dataset design

The municipality wants predictive analytics, not just the historical reporting
Weeks 1-3 built. We chose **hourly taxi demand per pickup zone** as the
prediction target: given a zone and an hour, predict how many pickups will
start there. Of the three options the assignment suggested (demand, trip
duration, fare amount), demand is the one that genuinely depends on all four
integrated datasets at once -- weather, air quality, zone and time all plausibly
move ridership, while fare is close to a deterministic function of
`trip_distance` under NYC's rate card, and duration sits in between. Choosing
demand also lets the model reuse exactly the join shape Week 2's
`weather_impact_summary` and `air_quality_impact_summary` product tables already
established: zone x hour as the grain, environment as the feature.

The training dataset is a **zone x hour grid**: one row per pickup zone per
clock hour over the analysis window, including hours where that zone recorded
zero pickups (`sql/ml/zone_hour_demand.sql`, built by
`urban_platform.ml.dataset.build_training_dataset`). This is the dataset's one
non-obvious design decision. The platform's existing `zone_weather_hours`
temp view (`analytics/context.py`) already does the same CROSS JOIN against an
hour spine for exactly this reason: a model trained only on rows where a trip
happened would never see the base rate of an idle zone-hour and would
over-predict everywhere. On the bootstrapped baseline (no Week 3 update release
applied; see Section 4) this grid has 571,613 rows, of which 58.7% are
zero-demand hours -- confirming the decision mattered, not just as a hypothetical.

The target variable, `trip_count`, is defined as the count of accepted,
validated taxi trips (Week 1-3's Bronze/Gold validation already happened
upstream) whose pickup hour and pickup zone match the row. Features are pulled
from the same Gold table at the same grain: the citywide hourly weather series
(temperature, humidity, precipitation, wind, condition code) and the
borough-level air-quality series (PM2.5, AQI, station count), both already
joined onto `pickup_hour`/`pickup_borough` by the Week 1 integration pipeline.
Zone, zone's borough and weather condition code are declared explicitly
categorical, following the same point Week 1's data catalog makes about
`PULocationID`/`DOLocationID` being location *codes*, not continuous
quantities. The split is **chronological**, not a random fraction: training on
days after the days being validated/tested would leak information no real
deployment would have (holidays, a cold front, a one-off event) from the future
into the past. Cutoffs are fixed calendar dates in `config/ml.yaml`
(validation starts 2024-03-01, test starts 2024-03-21) rather than computed as
a fraction of whatever range happens to be loaded, so re-running this against a
longer Gold table extends the test window forward instead of silently
rescaling it.

<!-- PAGEBREAK -->

## 2. Feature engineering strategy

`urban_platform.ml.features.build_feature_pipeline` returns one Spark ML
`Pipeline` built from five stages: an `SQLTransformer` that derives
`hour_of_day`, `day_of_week`, `month` and `is_weekend` from the row's local
hour; three `StringIndexer`s (zone, borough, weather condition code); an
`Imputer` (median) over the numeric weather/air-quality columns; a
`VectorAssembler`; and a `StandardScaler`. Every stage is a standard
Transformer/Estimator, so the whole feature stage is one object: `fit` once,
`save`/`load` as a unit, and reusable unchanged if a new numeric or categorical
column is added to the input list.

Categorical encoding needed the least new thinking -- it is the same rule Week 1
already established, just applied to three more columns. Missing-value handling
needed the most, and turned up a real finding rather than a hypothetical one.
Two Gold columns, `humidity_pct_v2` and `aqi_max`, are schema fields the Week 3
incremental platform only ever populates from an *applied update release*
(`config/datasets.yaml`'s `allowed_raw_additions`); a pure `--bootstrap`
baseline leaves them 100% NULL, not partially missing. A median `Imputer`
cannot produce a surrogate from an entirely empty column -- Spark raises
`surrogate cannot be computed` -- and there is nothing meaningful to impute
even if it could, since an all-NULL column carries no information. Both columns
are excluded from the model's feature list (`features.py: NUMERIC_COLUMNS`)
for that reason, while remaining in the Task 1 dataset's selected columns for
completeness and for whichever consumer applies an update release before
training. This is a different failure mode from air quality's genuine,
structural coverage gap -- Manhattan and Staten Island have no PM2.5 monitoring
station at all (a Week 1 finding, not a Week 4 one), so `pm25_ug_m3` and
`aqi_max` are NULL for roughly a third of all zone-hours regardless of release
state. Dropping those rows would silently delete two boroughs from the training
set; imputing them keeps every zone represented and lets the evaluation report
state, measured, what fraction of rows that decision affects.

Scaling (`StandardScaler`) is kept even though the primary model (gradient-
boosted trees) is scale-invariant -- tree splits do not change under a monotone
rescaling of a numeric feature. It is in the pipeline because the pipeline is
meant to be reusable with a different estimator (Task 3's discussion), and a
linear or distance-based model dropped into the same `Pipeline` would need it.

## 3. Reproducible ML pipeline

`urban_platform.ml.train.train_and_evaluate` appends a `GBTRegressor`
(labelCol=`trip_count`) as the **final stage of the same `Pipeline`** object
from Section 2, rather than fitting features and model separately and gluing
them back together by hand. `pipeline.fit(train)` therefore returns one
`PipelineModel` -- feature engineering and model together -- which
`model.write().overwrite().save(...)` persists as a single artifact under
`data/ml/models/<run_id>/`, and `PipelineModel.load(...)` reloads whole. This is
what makes retraining a non-special operation: there is no second code path to
keep in sync with the first, no risk that a saved model silently expects
features built by an older version of the pipeline. Retraining on new data is
calling `train_and_evaluate` again with a fresh `run_id` against whatever Gold
version is current; the function does not know or care whether that Gold table
came from a bootstrap or an incremental Week 3 update.

Evaluation uses `RegressionEvaluator` for RMSE, MAE and R^2 against both the
validation and the held-out test split, computed by `model.transform(...)` on
data the pipeline was not fit on. The measured results are in the Evaluation
Report (Section "Model evaluation").

Which parts are reusable: the entire `Pipeline` object is, as an indivisible
unit, by construction. New features are incorporated by adding a column name
to `NUMERIC_COLUMNS`/`CATEGORICAL_COLUMNS`/the `SQLTransformer` statement in
`features.py` -- no stage needs to change shape, only its input list.
Multiple prediction tasks (duration, fare) would reuse every stage except the
final `GBTRegressor` and its `labelCol`; `build_feature_pipeline(model_stage=,
label_col=)` already takes both as parameters for exactly that reason, so a
second task is a second call with different arguments, not new pipeline code.

<!-- PAGEBREAK -->

## 4. Engineering decisions and trade-offs

**Reusing analysis views instead of re-deriving integration logic.**
`dataset.build_training_dataset` calls the existing
`register_analysis_views`/`read_integrated` from `analytics/context.py` and
only adds a new SQL query and a split column on top -- nothing in Weeks 1-3 was
modified. The trade-off is that the ML dataset inherits every upstream
assumption those views encode (the NYC-timezone-only guard, the approved
weather-as-UTC interpretation, the `latest_trip_day` window), which is the
point: a bug fix or a schema-evolution change to Bronze/Gold propagates to the
training dataset automatically, rather than needing a second fix in a parallel
"ML ingestion" path.

**Bootstrap baseline, not an applied update release.** Training uses the
strict `--bootstrap` baseline (Week 3's `bootstrap_platform`) rather than a
state with an update release applied, to keep the dataset's row counts and
split boundaries independent of which update-release scenario happens to be
loaded locally. The direct cost is Section 2's finding: two schema columns
are structurally unavailable under this choice. We judged that documenting and
excluding them cleanly was preferable to training against a release-dependent
snapshot that would make the reported numbers non-reproducible for a grader
without that exact release applied.

**Chronological split over a random split.** A random train/test split would
very likely have scored better (nearby hours of the same zone are correlated,
so random splitting leaks neighbours of test rows into training) and would
have been simpler to implement. It was rejected because it answers a different
question than the one a demand-forecasting deployment actually faces: can the
model generalise to days it has not seen, not can it interpolate between days
it has. The reported test metrics are therefore a deliberately harder, more
representative number than a random split would have produced.

**GBTRegressor over a linear baseline.** Taxi demand is non-linear in its
inputs (rush-hour and borough interactions, zero-inflation from idle zones);
gradient-boosted trees handle both without manual interaction terms or a
separate zero-inflation model, at the cost of a less interpretable model than
linear regression would give. `StandardScaler` stays in the pipeline (Section
2) specifically so a future linear baseline can be swapped in by changing the
final stage only.

**Approach A/B comparison isolates dataset construction, not modelling.**
Both approaches in Task 4 (`urban_platform.ml.compare`) feed the *same*
feature pipeline and the *same* `GBTRegressor` configuration; the only
independent variable is how the zone-hour-demand table was produced --
re-derived from raw files (Approach A) or queried from the already-validated,
already-integrated Gold table (Approach B). This was a deliberate scoping
choice: comparing two different models trained on two different datasets would
conflate "which dataset is better" with "which model is better," and the
assignment's question is specifically about the role of the data-engineering
layer, not about model selection.
