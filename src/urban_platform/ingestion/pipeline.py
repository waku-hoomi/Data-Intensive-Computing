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
import json
import os
import re
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

from urban_platform.ingestion.quality import apply_quality_checks, validate_records
from urban_platform.ingestion.run_metadata import IngestionRunMetadata, Timer, write_metadata
from urban_platform.utils.config import DatasetConfig, PlatformConfig


class SchemaValidationError(Exception):
    pass


def _load_raw(spark: SparkSession, cfg: DatasetConfig, raw_data_dir: str,
              raw_path: str | None = None) -> DataFrame:
    path = raw_path or os.path.join(raw_data_dir, cfg.input_path)
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


def _validate_raw_schema(df: DataFrame, cfg: DatasetConfig, *, strict: bool = False,
                         baseline_schema: dict | None = None) -> None:
    if not cfg.expected_raw_columns:
        return
    actual = set(df.columns)
    missing = [c for c in cfg.expected_raw_columns if c not in actual]
    if missing:
        raise SchemaValidationError(
            f"Dataset '{cfg.name}': raw source is missing expected columns {missing}. "
            f"Actual columns: {sorted(actual)}"
        )
    if strict:
        unexpected = actual - set(cfg.expected_raw_columns) - set(cfg.allowed_raw_additions)
        if unexpected:
            raise SchemaValidationError(f"Dataset '{cfg.name}': unknown raw columns {sorted(unexpected)}")
        current_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        previous_types = baseline_schema or {}
        for column, previous_type in previous_types.items():
            if column in current_types and current_types[column] != previous_type:
                raise SchemaValidationError(
                    f"Dataset '{cfg.name}': incompatible raw type for {column}: "
                    f"expected {previous_type}, got {current_types[column]}")
        for column, declared_type in cfg.allowed_raw_additions.items():
            if column in current_types and current_types[column] != declared_type:
                raise SchemaValidationError(
                    f"Dataset '{cfg.name}': incompatible additive type for {column}: "
                    f"expected {declared_type}, got {current_types[column]}")


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


def _raw_schema_path(platform_cfg: PlatformConfig, name: str) -> str:
    return os.path.join(platform_cfg.metadata_dir, "raw_schemas", f"{name}.json")


def _read_raw_schema(platform_cfg: PlatformConfig, name: str) -> dict | None:
    path = _raw_schema_path(platform_cfg, name)
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def _save_raw_schema(platform_cfg: PlatformConfig, name: str, df: DataFrame) -> None:
    path = _raw_schema_path(platform_cfg, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    with open(path + ".tmp", "w", encoding="utf-8") as stream:
        json.dump(types, stream, indent=2, sort_keys=True)
    os.replace(path + ".tmp", path)


def _bronze_path(platform_cfg: PlatformConfig, name: str) -> str:
    return os.path.join(platform_cfg.bronze_dir, name)


def bronze_table_version(spark: SparkSession, platform_cfg: PlatformConfig,
                         name: str) -> int | None:
    path = _bronze_path(platform_cfg, name)
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return int(DeltaTable.forPath(spark, path).history(1).select("version").first()[0])


def _references(spark: SparkSession, platform_cfg: PlatformConfig,
                cfg: DatasetConfig) -> dict[str, DataFrame]:
    return {rule.dataset: spark.read.format("delta").load(_bronze_path(platform_cfg, rule.dataset))
            for rule in cfg.foreign_keys}


def _quarantine(spark: SparkSession, platform_cfg: PlatformConfig, name: str,
                run_id: str, rejected: DataFrame, source_path: str) -> str:
    safe_run_id = re.sub(r"[^A-Za-z0-9_-]", "_", run_id)
    if not safe_run_id:
        raise ValueError("run_id must contain a letter or digit")
    path = os.path.join(platform_cfg.metadata_dir, "quarantine", name, safe_run_id)
    (rejected.withColumn("_run_id", F.lit(run_id))
     .withColumn("_source_path", F.lit(source_path))
     .write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(path))
    return path


def _base_metrics(platform_cfg: PlatformConfig, name: str, before_version: int | None) -> dict:
    return dict(dataset_name=name, status="success", processed=0, inserted=0, updated=0,
                duplicates=0, rejected=0, schema_version=platform_cfg.schema_version,
                changed_keys_path=None, quarantine_path=None,
                before_version=before_version, after_version=before_version,
                validation_failures=[], execution_seconds=0.0,
                validation_seconds=0.0, merge_seconds=0.0)


def bootstrap_validated_baseline(spark: SparkSession,
                                 platform_cfg: PlatformConfig) -> dict[str, dict]:
    """Strictly recheck historical raw files and replace each Bronze baseline.

    The lookup is written first so taxi foreign keys can be checked. A
    failure is reported per dataset; the caller must not publish a Gold
    snapshot unless every entry succeeded.
    """
    order = sorted(platform_cfg.datasets, key=lambda item: (item.name != "taxi_zone_lookup", item.name))
    results = {}
    for cfg in order:
        started = time.perf_counter()
        before = bronze_table_version(spark, platform_cfg, cfg.name)
        metrics = _base_metrics(platform_cfg, cfg.name, before)
        try:
            raw = _load_raw(spark, cfg, platform_cfg.raw_data_dir)
            validation_started = time.perf_counter()
            _validate_raw_schema(raw, cfg, strict=True)
            standardized = cfg.resolve_transform_fn()(raw)
            clean, rejected, report = validate_records(
                standardized, cfg, _references(spark, platform_cfg, cfg))
            metrics["validation_seconds"] = round(time.perf_counter() - validation_started, 3)
            metrics["processed"] = report.input_rows
            metrics["duplicates"] = report.rejected_by_reason.get("duplicate_primary_key", 0)
            metrics["rejected"] = report.rejected_rows - metrics["duplicates"]
            metrics["inserted"] = report.output_rows
            metrics["validation_failures"] = report.rejected_by_reason
            metrics["quarantine_path"] = _quarantine(
                spark, platform_cfg, cfg.name, "baseline", rejected,
                os.path.join(platform_cfg.raw_data_dir, cfg.input_path))
            writer = (clean.write.format("delta").mode("overwrite")
                      .option("overwriteSchema", "true")
                      .option("delta.enableChangeDataFeed", "true"))
            if cfg.partition_by:
                writer = writer.partitionBy(*cfg.partition_by)
            writer.save(_bronze_path(platform_cfg, cfg.name))
            _save_raw_schema(platform_cfg, cfg.name, raw)
            metrics["after_version"] = bronze_table_version(spark, platform_cfg, cfg.name)
        except Exception as exc:  # keep other historical datasets inspectable
            metrics["status"] = "failed"
            metrics["validation_failures"] = [f"{type(exc).__name__}: {exc}"]
        metrics["execution_seconds"] = round(time.perf_counter() - started, 3)
        results[cfg.name] = metrics
    return results


def apply_incremental_dataset(
    spark: SparkSession,
    platform_cfg: PlatformConfig,
    dataset_name: str,
    raw_path: str,
    run_id: str,
) -> tuple[dict, DataFrame | None]:
    """Validate one update and MERGE it into Bronze, returning changed keys.

    A failed dataset returns status='failed' and leaves other datasets free
    to ingest. The orchestrator publishes only if all datasets succeeded.
    """
    cfg = platform_cfg.get_dataset(dataset_name)
    started = time.perf_counter()
    target_path = _bronze_path(platform_cfg, dataset_name)
    before = bronze_table_version(spark, platform_cfg, dataset_name)
    metrics = _base_metrics(platform_cfg, dataset_name, before)
    if before is None:
        metrics.update(status="failed", validation_failures=["validated baseline is missing"])
        metrics["execution_seconds"] = round(time.perf_counter() - started, 3)
        return metrics, None
    try:
        raw = _load_raw(spark, cfg, platform_cfg.raw_data_dir, raw_path)
        validation_started = time.perf_counter()
        baseline_schema = _read_raw_schema(platform_cfg, dataset_name)
        if baseline_schema is None:
            raise SchemaValidationError(f"{dataset_name}: historical raw schema manifest is missing")
        _validate_raw_schema(raw, cfg, strict=True, baseline_schema=baseline_schema)
        standardized = cfg.resolve_transform_fn()(raw)
        clean, rejected, report = validate_records(
            standardized, cfg, _references(spark, platform_cfg, cfg))
        metrics["validation_seconds"] = round(time.perf_counter() - validation_started, 3)
        metrics["processed"] = report.input_rows
        batch_duplicates = report.rejected_by_reason.get("duplicate_primary_key", 0)
        metrics["duplicates"] = batch_duplicates
        metrics["rejected"] = report.rejected_rows - batch_duplicates
        metrics["validation_failures"] = report.rejected_by_reason
        target = spark.read.format("delta").load(target_path)
        join_condition = F.lit(True)
        for key in cfg.primary_key:
            join_condition = join_condition & (F.col(f"s.`{key}`") == F.col(f"t.`{key}`"))
        joined = clean.alias("s").join(target.alias("t"), on=join_condition, how="left")
        # Use a non-null marker from the target rather than a nullable value
        # column, since null payload fields are legitimate.
        exists = F.col(f"t.{cfg.primary_key[0]}").isNotNull()
        if dataset_name == "taxi_trips":
            different = F.lit(False)  # hash of the complete raw row is its only stable identity
        else:
            comparisons = []
            for column in clean.columns:
                if column in cfg.primary_key:
                    continue
                left = F.col(f"s.`{column}`")
                right = F.col(f"t.`{column}`") if column in target.columns else F.lit(None).cast(clean.schema[column].dataType)
                comparisons.append(~left.eqNullSafe(right))
            different = F.lit(False)
            for predicate in comparisons:
                different = different | predicate
        classified = joined.select("s.*", exists.alias("_exists"), different.alias("_different"))
        safe_run_id = re.sub(r"[^A-Za-z0-9_-]", "_", run_id)
        attempt = f"before_{before}"
        classified_path = os.path.join(platform_cfg.metadata_dir, "classified", safe_run_id, dataset_name, attempt)
        # One stable Delta staging snapshot avoids repeated joins against a
        # changing Bronze target and makes the MERGE source independent.
        (classified.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").save(classified_path))
        classified = spark.read.format("delta").load(classified_path)
        metrics["classified_path"] = classified_path
        status_counts = (classified.groupBy("_exists", "_different").count().collect())
        for row in status_counts:
            if not row["_exists"]:
                metrics["inserted"] += row["count"]
            elif row["_different"]:
                metrics["updated"] += row["count"]
            else:
                metrics["duplicates"] += row["count"]
        cross_duplicates = (classified.filter(F.col("_exists") & ~F.col("_different"))
                            .select(*clean.columns)
                            .withColumn("_reject_reason", F.lit("cross_batch_duplicate")))
        metrics["validation_failures"]["cross_batch_duplicate"] = (
            metrics["duplicates"] - batch_duplicates)
        metrics["quarantine_path"] = _quarantine(
            spark, platform_cfg, dataset_name, run_id,
            rejected.unionByName(cross_duplicates), raw_path)
        changed = classified.filter(~F.col("_exists") | F.col("_different"))
        changed_path = os.path.join(platform_cfg.metadata_dir, "change_keys", safe_run_id, dataset_name, attempt)
        (changed.select(*cfg.primary_key).distinct().write.format("delta")
         .mode("overwrite").option("overwriteSchema", "true").save(changed_path))
        metrics["changed_keys_path"] = changed_path
        if metrics["inserted"] or metrics["updated"]:
            merge_started = time.perf_counter()
            source = changed.select(*clean.columns)
            merge_condition = " AND ".join(f"t.`{key}` = s.`{key}`" for key in cfg.primary_key)
            assignments = {column: f"s.`{column}`" for column in clean.columns}
            updates = {column: value for column, value in assignments.items()
                       if column not in cfg.primary_key}
            previous_auto_merge = spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled", "false")
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            try:
                builder = DeltaTable.forPath(spark, target_path).alias("t").merge(source.alias("s"), merge_condition)
                if hasattr(builder, "withSchemaEvolution"):
                    builder = builder.withSchemaEvolution()
                builder.whenMatchedUpdate(set=updates).whenNotMatchedInsert(values=assignments).execute()
            finally:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", previous_auto_merge)
            metrics["merge_seconds"] = round(time.perf_counter() - merge_started, 3)
        metrics["after_version"] = bronze_table_version(spark, platform_cfg, dataset_name)
        metrics["execution_seconds"] = round(time.perf_counter() - started, 3)
        return metrics, spark.read.format("delta").load(changed_path)
    except Exception as exc:
        metrics["status"] = "failed"
        metrics["validation_failures"] = [f"{type(exc).__name__}: {exc}"]
        metrics["after_version"] = bronze_table_version(spark, platform_cfg, dataset_name)
        metrics["execution_seconds"] = round(time.perf_counter() - started, 3)
        return metrics, None


def changed_keys_since_publication(
    spark: SparkSession,
    platform_cfg: PlatformConfig,
    dataset_name: str,
    published_version: int,
) -> DataFrame | None:
    """Recover committed source keys after a failed, unpublished run.

    Bronze commits can succeed before a later dataset, Gold, or product stage
    fails. On retry the same rows are duplicates, so their current-batch key
    frame is empty. CDF bridges the last published Bronze version to the
    current one and makes that retry complete the pending snapshot.
    """
    cfg = platform_cfg.get_dataset(dataset_name)
    path = _bronze_path(platform_cfg, dataset_name)
    current = bronze_table_version(spark, platform_cfg, dataset_name)
    if current is None or current <= published_version:
        return None
    try:
        changes = (spark.read.format("delta").option("readChangeFeed", "true")
                   .option("startingVersion", published_version + 1)
                   .option("endingVersion", current).load(path))
    except Exception as exc:
        raise RuntimeError(
            f"{dataset_name}: cannot recover unpublished Bronze changes from "
            f"versions {published_version + 1}-{current}; rebuild the strict baseline"
        ) from exc
    return (changes.filter(F.col("_change_type").isin("insert", "update_postimage"))
            .select(*cfg.primary_key).distinct())
