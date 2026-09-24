"""Validated baseline and incremental batch orchestration."""
from __future__ import annotations

import datetime as dt
import json
import time
import uuid
from pathlib import Path

from pyspark.sql import SparkSession

from urban_platform.analytics.context import analytics_config, effective_analysis_config
from urban_platform.analytics.products import PRODUCTS, build_products
from urban_platform.analytics.refresh import refresh_affected_products
from urban_platform.ingestion.pipeline import (
    apply_incremental_dataset, bootstrap_validated_baseline,
    changed_keys_since_publication,
)
from urban_platform.incremental.gold import (
    change_feed_enabled, enable_gold_change_feed, gold_changes, gold_path,
    refresh_integrated_gold, current_version,
)
from urban_platform.integration.integrate import write_integrated_taxi_trips
from urban_platform.monitoring.events import append_run_event, utc_now
from urban_platform.monitoring.publication import latest_publication, publish_snapshot
from urban_platform.utils.config import PlatformConfig


UPDATE_FILES = {
    "taxi_zone_lookup": "taxi_zone_lookup.csv",
    "weather": "weather.csv",
    "air_quality": "air_quality.csv",
    "taxi_trips": "taxi_trips.parquet",
}
ORDER = tuple(UPDATE_FILES)


def new_run_id() -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"week3-{stamp}-{uuid.uuid4().hex[:8]}"


def _write_artifact(cfg: PlatformConfig, result: dict) -> None:
    root = Path(cfg.metadata_dir).parents[1]
    destination = root / "artifacts/week3" / f"{result['run_id']}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")


def _record(
    spark: SparkSession, cfg: PlatformConfig, result: dict, stage: str,
    started: str, elapsed: float, *, dataset: str | None = None,
    status: str = "success", metrics: dict | None = None, error: str | None = None,
) -> None:
    if not result.get("monitoring_enabled", True):
        return
    overhead = append_run_event(
        spark, cfg, result["run_id"], stage, started, elapsed,
        dataset_name=dataset, status=status, metrics=metrics, error=error,
    )
    result["monitoring_write_seconds"] += overhead


def _bootstrap_platform_impl(spark: SparkSession, cfg: PlatformConfig, run_id: str) -> dict:
    """Revalidate all supplied history before publishing a Week 3 baseline."""
    result = {"run_id": run_id, "kind": "baseline",
              "started_at_utc": utc_now(), "monitoring_write_seconds": 0.0}
    start = time.perf_counter()
    stage_started = utc_now()
    bronze = bootstrap_validated_baseline(spark, cfg)
    result["bronze"] = bronze
    for name, metrics in bronze.items():
        _record(spark, cfg, result, "bronze", stage_started,
                float(metrics.get("execution_seconds", 0.0)), dataset=name,
                status=metrics["status"], metrics=metrics)
    if any(metrics["status"] != "success" for metrics in bronze.values()):
        result["status"] = "failed"
        result["error"] = "One or more historical datasets failed strict validation"
        result["total_seconds"] = time.perf_counter() - start
        _write_artifact(cfg, result)
        return result

    stage_started = utc_now()
    stage_time = time.perf_counter()
    path = write_integrated_taxi_trips(spark, cfg)
    # The property is set on table creation by the Gold writer; this also
    # upgrades an older table if the Delta runtime ignored the writer option.
    if not change_feed_enabled(spark, path):
        enable_gold_change_feed(spark, cfg)
    result["gold"] = {"path": path, "seconds": time.perf_counter() - stage_time}
    _record(spark, cfg, result, "gold", stage_started, result["gold"]["seconds"])

    stage_started = utc_now()
    stage_time = time.perf_counter()
    products = build_products(spark, cfg)
    result["products"] = products
    _record(spark, cfg, result, "products", stage_started, time.perf_counter() - stage_time,
            metrics={"inserted": sum(p["row_count"] for p in products)})
    analysis = effective_analysis_config(
        spark.read.format("delta").load(gold_path(cfg)), analytics_config()["analysis"]
    )
    result["publication"] = publish_snapshot(spark, cfg, result["run_id"], analysis)
    result["status"] = "success"
    result["total_seconds"] = time.perf_counter() - start
    _write_artifact(cfg, result)
    return result


def bootstrap_platform(spark: SparkSession, cfg: PlatformConfig, run_id: str | None = None) -> dict:
    """Run the strict baseline and always leave a durable failure artifact."""
    actual_run_id = run_id or new_run_id()
    started = time.perf_counter()
    try:
        return _bootstrap_platform_impl(spark, cfg, actual_run_id)
    except Exception as exc:
        result = {
            "run_id": actual_run_id, "kind": "baseline", "status": "failed",
            "started_at_utc": utc_now(), "monitoring_write_seconds": 0.0,
            "error": f"{type(exc).__name__}: {exc}",
            "total_seconds": time.perf_counter() - started,
        }
        try:
            _record(spark, cfg, result, "orchestrator", result["started_at_utc"],
                    result["total_seconds"], status="failed", error=result["error"])
        except Exception as monitoring_exc:
            result["monitoring_error"] = f"{type(monitoring_exc).__name__}: {monitoring_exc}"
        _write_artifact(cfg, result)
        return result


def process_updates(
    spark: SparkSession, cfg: PlatformConfig, updates_dir: str | Path,
    run_id: str | None = None,
    *, refresh_mode: str = "incremental", monitoring_enabled: bool = True,
) -> dict:
    """Process four independent update files, then atomically publish a pointer."""
    if refresh_mode not in {"incremental", "full"}:
        raise ValueError(f"Unknown refresh mode: {refresh_mode}")
    previous = latest_publication(spark, cfg)
    if previous is None:
        raise ValueError("Run the strict Week 3 baseline before processing updates")
    updates_dir = Path(updates_dir).resolve()
    missing = [name for name, filename in UPDATE_FILES.items()
               if not (updates_dir / filename).exists()]
    if missing:
        raise FileNotFoundError(f"Missing update files for: {missing}")
    result = {"run_id": run_id or new_run_id(), "kind": "incremental",
              "started_at_utc": utc_now(), "monitoring_write_seconds": 0.0,
              "previous_publication_run": previous["run_id"],
              "refresh_mode": refresh_mode, "monitoring_enabled": monitoring_enabled}
    started = time.perf_counter()
    metrics_by_name = {}
    keys_by_name = {}
    try:
        for name in ORDER:
            stage_started = utc_now()
            stage_time = time.perf_counter()
            metrics, keys = apply_incremental_dataset(
                spark, cfg, name, str(updates_dir / UPDATE_FILES[name]), result["run_id"]
            )
            elapsed = time.perf_counter() - stage_time
            metrics_by_name[name] = metrics
            if metrics["status"] == "success":
                # CDF covers both the current batch and any earlier unpublished
                # commits. A non-empty new batch must not hide pending keys.
                pending = changed_keys_since_publication(
                    spark, cfg, name, int(previous["bronze_versions"][name])
                )
                if pending is not None:
                    keys = pending if keys is None else pending.unionByName(keys).distinct()
                    metrics["unpublished_change_keys"] = keys.count()
            keys_by_name[name] = keys
            _record(spark, cfg, result, "bronze", stage_started,
                    float(metrics.get("execution_seconds", elapsed)), dataset=name,
                    status=metrics["status"], metrics=metrics)
        result["bronze"] = metrics_by_name
        if any(record["status"] != "success" for record in metrics_by_name.values()):
            raise RuntimeError("At least one dataset failed; the prior snapshot stays published")

        if not change_feed_enabled(spark, gold_path(cfg)):
            enable_gold_change_feed(spark, cfg)
        stage_started = utc_now()
        stage_time = time.perf_counter()
        if refresh_mode == "full":
            before = current_version(spark, gold_path(cfg))
            write_integrated_taxi_trips(spark, cfg)
            gold = {"before_version": before,
                    "after_version": current_version(spark, gold_path(cfg)),
                    "affected_trips": spark.read.format("delta").load(gold_path(cfg)).count()}
        else:
            gold = refresh_integrated_gold(spark, cfg, keys_by_name)
        gold["seconds"] = time.perf_counter() - stage_time
        result["gold"] = gold
        _record(spark, cfg, result, "gold", stage_started, gold["seconds"],
                metrics={"processed": gold["affected_trips"],
                         "before_version": gold["before_version"],
                         "after_version": gold["after_version"]})

        # Products may lag an already-committed Gold table after an interruption.
        # Include every commit since the reader-visible publication, even when
        # this retry writes identical values or follows a partially refreshed product.
        changes = gold_changes(spark, cfg, int(previous["gold_version"]), gold["after_version"])
        stage_started = utc_now()
        if refresh_mode == "full":
            products = {}
            for name in PRODUCTS:
                product_started = time.perf_counter()
                build_products(spark, cfg, [name])
                products[name] = {"mode": "full", "seconds": time.perf_counter() - product_started}
        else:
            products = refresh_affected_products(
                spark, cfg, changes, metrics_by_name, previous["analysis_config"]
            )
        result["products"] = products
        for name, item in products.items():
            _record(spark, cfg, result, "product", stage_started, item["seconds"],
                    dataset=name, status=item["mode"], metrics=item)

        analysis = effective_analysis_config(
            spark.read.format("delta").load(gold_path(cfg)), analytics_config()["analysis"]
        )
        result["publication"] = publish_snapshot(spark, cfg, result["run_id"], analysis)
        result["status"] = "success"
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        try:
            _record(spark, cfg, result, "orchestrator", utc_now(), 0.0,
                    status="failed", error=result["error"])
        except Exception as monitoring_exc:
            result["monitoring_error"] = f"{type(monitoring_exc).__name__}: {monitoring_exc}"
    result["total_seconds"] = time.perf_counter() - started
    _write_artifact(cfg, result)
    return result
