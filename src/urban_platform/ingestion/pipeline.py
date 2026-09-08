"""The generic ingestion engine (Task 3).

This module is the reusable core of the platform: it knows nothing about
taxi trips, weather, or air quality specifically. It only knows how to:
  1. load a file (csv/parquet) per a DatasetConfig,
  2. validate the raw schema against the columns we expect from the source,
  3. hand off to the dataset's one small transform function (standardize
     names/types/timestamps -- Task 4's common data model),
  4. run generic data-quality checks (quality.py),
  5. write the result as a Delta table (partitioned per config),
  6. record ingestion metadata (run_metadata.py).

Adding a new dataset means adding one block to config/datasets.yaml and,
only if its raw shape needs real reshaping, one transform function. No
change to this file is required -- this is the answer to Task 3's
"if the municipality adds 20 new datasets next year" question.
"""
import datetime as dt
import os

from pyspark.sql import DataFrame, SparkSession

from urban_platform.ingestion.quality import apply_quality_checks
from urban_platform.ingestion.run_metadata import IngestionRunMetadata, Timer, write_metadata
from urban_platform.utils.config import DatasetConfig, PlatformConfig


class SchemaValidationError(Exception):
    pass


def _load_raw(spark: SparkSession, cfg: DatasetConfig, raw_data_dir: str) -> DataFrame:
    path = os.path.join(raw_data_dir, cfg.input_path)
    if cfg.input_format == "csv":
        # inferSchema is deliberately OFF: Spark's type sniffer misreads
        # ambiguous source columns (e.g. EPA's "Time GMT" column of plain
        # "HH:MM" strings gets inferred as a full timestamp stamped with
        # today's date, silently corrupting values). Every dataset's
        # transform function already casts each column explicitly per the
        # common data model (Task 4), so reading as string here is both
        # safer and sufficient.
        options = {"header": "true", "inferSchema": "false"}
        options.update(cfg.csv_options)
        return spark.read.options(**options).csv(path)
    elif cfg.input_format == "parquet":
        return spark.read.parquet(path)
    else:
        raise ValueError(f"Unsupported input_format '{cfg.input_format}' for dataset '{cfg.name}'")


def _validate_raw_schema(df: DataFrame, cfg: DatasetConfig) -> None:
    if not cfg.expected_raw_columns:
        return
    actual = set(df.columns)
    missing = [c for c in cfg.expected_raw_columns if c not in actual]
    if missing:
        raise SchemaValidationError(
            f"Dataset '{cfg.name}': raw source is missing expected columns {missing}. "
            f"Actual columns: {sorted(actual)}"
        )


def ingest_dataset(spark: SparkSession, platform_cfg: PlatformConfig, dataset_name: str) -> IngestionRunMetadata:
    """Run the full generic pipeline for one dataset and write it as Delta.

    Returns the metadata record (also appended to the ingestion log).
    """
    cfg = platform_cfg.get_dataset(dataset_name)
    output_path = os.path.join(platform_cfg.bronze_dir, cfg.name)
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()

    status = "success"
    error_msg = None
    input_rows = output_rows = rejected_rows = 0
    rejected_by_reason = {}

    with Timer() as timer:
        try:
            raw_df = _load_raw(spark, cfg, platform_cfg.raw_data_dir)
            _validate_raw_schema(raw_df, cfg)

            standardized_df = cfg.resolve_transform_fn()(raw_df)
            clean_df, report = apply_quality_checks(standardized_df, cfg)

            input_rows = report.input_rows
            output_rows = report.output_rows
            rejected_rows = report.rejected_rows
            rejected_by_reason = report.rejected_by_reason

            writer = clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if cfg.partition_by:
                writer = writer.partitionBy(*cfg.partition_by)
            writer.save(output_path)
        except Exception as exc:  # noqa: BLE001 -- deliberately broad: we want
            # every dataset-level failure captured in the metadata log rather
            # than crashing the whole batch (Task 3: "generate ingestion
            # metadata" implies a run must always produce a record).
            status = "failed"
            error_msg = f"{type(exc).__name__}: {exc}"

    ended_at = dt.datetime.now(dt.timezone.utc).isoformat()
    record = IngestionRunMetadata(
        dataset_name=cfg.name,
        run_started_at=started_at,
        run_ended_at=ended_at,
        execution_seconds=round(timer.elapsed, 3),
        schema_version=platform_cfg.schema_version,
        input_rows=input_rows,
        output_rows=output_rows,
        rejected_rows=rejected_rows,
        rejected_by_reason=rejected_by_reason,
        output_path=output_path,
        status=status,
        error=error_msg,
    )
    write_metadata(platform_cfg.metadata_dir, record)

    if status == "failed":
        raise RuntimeError(f"Ingestion failed for dataset '{cfg.name}': {error_msg}")

    return record


def ingest_all(spark: SparkSession, platform_cfg: PlatformConfig) -> list:
    records = []
    for cfg in platform_cfg.datasets:
        records.append(ingest_dataset(spark, platform_cfg, cfg.name))
    return records