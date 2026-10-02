"""Task 2: a reusable feature-engineering pipeline over the demand dataset.

Every step is a standard Spark ML `Transformer`, so the whole feature stage is
one object that can be `fit`, `save`, `load` and re-used on a new training set
without editing code. The design rule followed here: categorical values are
never handed to the model as raw numbers.

`pu_location_id`, `weather_condition_code` and `aq_station_count` are stored as
integers but are *codes*, not quantities -- taxi zone 132 is not "twice" zone 66
and weather code 3 is not "one worse" than code 2. Feeding them in unscaled
would let a tree split on a meaningless ordinal axis, so they go through
`StringIndexer` first. This is the same point the data catalog makes about
`PULocationID`/`DOLocationID` in Week 1.

Missing values are handled with a median `Imputer` rather than dropped rows.
The only structurally-missing column is air quality, which has no monitoring
station in Manhattan or Staten Island: dropping those rows would silently delete
two entire boroughs, i.e. exactly the bias the feature analysis is meant to
expose. Imputing instead keeps them and lets the evaluation report the cost.
"""
from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, SQLTransformer, StandardScaler, StringIndexer, VectorAssembler

# Columns that carry a numeric scale and may contain genuine NULLs.
#
# humidity_pct_v2 and aqi_max are deliberately excluded even though the Gold
# schema has them: both are additive fields the Week 3 incremental platform
# only ever populates from an applied update release (see
# config/datasets.yaml allowed_raw_additions). A pure `--bootstrap` baseline
# -- what this model is trained against -- leaves them 100% NULL, not
# partially missing like pm25_ug_m3. A median Imputer cannot produce a
# surrogate value from an entirely-empty column (Spark raises
# "surrogate cannot be computed"), and there would be nothing for it to
# impute from even if it could: an all-NULL column carries zero information,
# unlike air quality's genuine per-borough coverage gap below.
NUMERIC_COLUMNS = [
    "temp_c",
    "relative_humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
    "pm25_ug_m3",
]

# Derived from local_hour; never NULL, so they are excluded from the imputer.
TEMPORAL_COLUMNS = ["hour_of_day", "day_of_week", "month", "is_weekend"]

# Stored as integer codes; indexed so the model cannot read them as ordinals.
CATEGORICAL_COLUMNS = ["pu_location_id", "pickup_borough", "weather_condition_code"]

_TEMPORAL_SQL = """
SELECT *,
       hour(local_hour)   AS hour_of_day,
       dayofweek(local_hour) AS day_of_week,
       month(local_hour)  AS month,
       CASE WHEN dayofweek(local_hour) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
       CAST(pu_location_id AS STRING) AS zone_key
FROM __THIS__
"""


def indexed_column(name: str) -> str:
    return f"{name}_idx"


def build_feature_pipeline(model_stage=None, label_col: str = "trip_count") -> Pipeline:
    """Assemble the feature stages, optionally followed by an estimator.

    Passing `model_stage` produces a single `Pipeline` whose `fit` returns one
    `PipelineModel` containing both the feature engineering and the trained
    model. That is what makes retraining a one-call operation: re-`fit` the same
    pipeline object on a newer dataset.
    """
    temporal = SQLTransformer(statement=_TEMPORAL_SQL)
    indexers = [
        StringIndexer(
            inputCol="zone_key" if name == "pu_location_id" else name,
            outputCol=indexed_column(name),
            handleInvalid="keep",
        )
        for name in CATEGORICAL_COLUMNS
    ]
    imputer = Imputer(
        inputCols=NUMERIC_COLUMNS,
        outputCols=[f"{name}_imputed" for name in NUMERIC_COLUMNS],
        strategy="median",
    )
    assembler = VectorAssembler(
        inputCols=[indexed_column(name) for name in CATEGORICAL_COLUMNS]
        + [f"{name}_imputed" for name in NUMERIC_COLUMNS]
        + TEMPORAL_COLUMNS,
        outputCol="features_raw",
        handleInvalid="keep",
    )
    # Scaling matters for the linear baseline in the evaluation report; for the
    # gradient-boosted trees used as the primary model it is monotone and so
    # does not change any split -- it is kept because the pipeline is meant to
    # be reusable with a different estimator.
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

    stages = [temporal, *indexers, imputer, assembler, scaler]
    if model_stage is not None:
        stages.append(model_stage)
    return Pipeline(stages=stages)


def feature_columns() -> list[str]:
    """The assembled feature vector's columns, in order."""
    return (
        [indexed_column(name) for name in CATEGORICAL_COLUMNS]
        + [f"{name}_imputed" for name in NUMERIC_COLUMNS]
        + TEMPORAL_COLUMNS
    )
