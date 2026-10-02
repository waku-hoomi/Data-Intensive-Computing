"""Task 3: a reproducible Spark MLlib training/evaluation entry point.

Both the feature engineering and the model live in one `Pipeline`, so `fit`
returns a single `PipelineModel` that can be saved, reloaded, and re-fit on new
data as an indivisible artifact. Retraining is therefore not a special code
path -- it is `train_and_evaluate` called again with a fresh `run_id` against an
updated dataset. The report explains which pieces are reusable, but the code
enforces it: there is no second pipeline to keep in sync.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession

from urban_platform.ml import dataset as dataset_module
from urban_platform.ml.features import build_feature_pipeline

ROOT = Path(__file__).resolve().parents[3]
ARTIFACTS = ROOT / "artifacts/week4"
MODELS = ROOT / "data/ml/models"


def model_path(run_id: str) -> Path:
    return MODELS / run_id


def evaluate_predictions(predictions: DataFrame, label_col: str) -> dict:
    metrics = {}
    for metric in ("rmse", "mae", "r2"):
        evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName=metric)
        metrics[metric] = evaluator.evaluate(predictions)
    return metrics


def train_and_evaluate(
    spark: SparkSession,
    platform_cfg,
    cfg: dict | None = None,
    run_id: str = "week4",
    training_df: DataFrame | None = None,
) -> dict:
    """Fit the full pipeline on the train split and score validation and test.

    `training_df` lets the Approach A/B comparison feed a differently-sourced
    table through the exact same feature and model code, so the only variable
    between the two runs is how the dataset was built.
    """
    cfg = cfg or dataset_module.ml_config()
    label_col = cfg["label_col"]
    if training_df is None:
        training_df = dataset_module.build_training_dataset(spark, platform_cfg, cfg)

    train = training_df.filter(training_df.split == "train")
    validation = training_df.filter(training_df.split == "validation")
    test = training_df.filter(training_df.split == "test")

    regressor = GBTRegressor(
        labelCol=label_col,
        featuresCol="features",
        maxIter=cfg["model"]["max_iter"],
        maxDepth=cfg["model"]["max_depth"],
        seed=cfg["model"]["seed"],
    )
    pipeline = build_feature_pipeline(model_stage=regressor, label_col=label_col)

    started = time.perf_counter()
    model = pipeline.fit(train)
    fit_seconds = time.perf_counter() - started

    output = model_path(run_id)
    model.write().overwrite().save(str(output))

    report = {
        "run_id": run_id,
        "fit_seconds": fit_seconds,
        "train_rows": train.count(),
        "validation_rows": validation.count(),
        "test_rows": test.count(),
        "features": model.stages[-1].getFeaturesCol(),
        "validation": evaluate_predictions(model.transform(validation), label_col),
        "test": evaluate_predictions(model.transform(test), label_col),
        "model_path": str(output),
    }
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / f"train_{run_id}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def load_model(run_id: str) -> PipelineModel:
    return PipelineModel.load(str(model_path(run_id)))
