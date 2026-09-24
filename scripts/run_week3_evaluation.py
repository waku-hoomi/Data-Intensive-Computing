"""Reproduce Week 3 full-versus-incremental measurements from one baseline.

The baseline snapshot and trial workspaces live below ignored
``data/evaluation``. Files are hard-linked on the same Windows volume, so each
trial starts from the same immutable Delta state without duplicating every
Parquet file. Delta appends new files and logs; it does not modify the linked
baseline files in place.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import urban_platform.analytics.products as product_module
from urban_platform.analytics.context import (
    QUERY_IDS, analytics_config, effective_analysis_config,
    register_analysis_views, sql_text,
)
from urban_platform.analytics.products import PRODUCTS
from urban_platform.analytics.verification import assert_same_results
from urban_platform.incremental.runner import UPDATE_FILES, process_updates
from urban_platform.monitoring.publication import latest_publication
from urban_platform.utils.config import load_config
from urban_platform.utils.spark_session import get_spark
from urban_platform.benchmark.production import fingerprint, storage_inventory, validation_pair

EVAL_ROOT = ROOT / "data" / "evaluation"
BASELINE = EVAL_ROOT / "baseline"


def _assert_eval_path(path: Path) -> Path:
    resolved = path.resolve()
    if EVAL_ROOT.resolve() not in resolved.parents:
        raise ValueError(f"Refusing to modify a path outside {EVAL_ROOT}: {resolved}")
    return resolved


def _safe_clear(path: Path) -> None:
    resolved = _assert_eval_path(path)
    if resolved.exists():
        shutil.rmtree(resolved)


def _link_or_copy(source: str, destination: str) -> str:
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)
    return destination


def _copy_tree(source: Path, destination: Path) -> None:
    _safe_clear(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, destination, copy_function=_link_or_copy)


def capture_baseline(replace_existing: bool = False) -> dict:
    if BASELINE.exists() and not replace_existing:
        raise FileExistsError(f"Baseline already exists: {BASELINE}; pass --replace-baseline")
    if BASELINE.exists():
        _safe_clear(BASELINE)
    BASELINE.mkdir(parents=True)
    copied = []
    for relative in (
        "data/bronze", "data/gold", "data/products", "data/lab2_metadata",
        "data/monitoring", "data/_metadata/raw_schemas",
    ):
        source = ROOT / relative
        if source.exists():
            target = BASELINE / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source, target, copy_function=_link_or_copy)
            copied.append(relative)
    manifest = {
        "source_root": str(ROOT), "copied": copied,
        "files": sum(1 for path in BASELINE.rglob("*") if path.is_file()),
        "bytes_logical": sum(path.stat().st_size for path in BASELINE.rglob("*") if path.is_file()),
    }
    (BASELINE / "baseline_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest


def _trial_config(base_config, trial_root: Path):
    return replace(
        base_config,
        bronze_dir=str(trial_root / "data/bronze"),
        gold_dir=str(trial_root / "data/gold"),
        metadata_dir=str(trial_root / "data/_metadata"),
    )


def _fingerprint(df) -> dict:
    return fingerprint(df)


def _verify_outputs(spark, incremental_root: Path, full_root: Path) -> dict:
    inc_gold = spark.read.format("delta").load(
        str(incremental_root / "data/gold/integrated_taxi_trips"))
    full_gold = spark.read.format("delta").load(
        str(full_root / "data/gold/integrated_taxi_trips"))
    inc_fingerprint = _fingerprint(inc_gold)
    full_fingerprint = _fingerprint(full_gold)
    if inc_fingerprint != full_fingerprint:
        raise AssertionError(
            f"Gold fingerprints differ: {inc_fingerprint} != {full_fingerprint}")

    product_checks = {}
    for name in PRODUCTS:
        inc = spark.read.format("delta").load(
            str(incremental_root / "data/products" / name)).collect()
        full = spark.read.format("delta").load(
            str(full_root / "data/products" / name)).collect()
        assert_same_results(inc, full)
        product_checks[name] = len(inc)

    query_checks = {}
    query_outputs = {}
    for label, root in (("incremental", incremental_root), ("full", full_root)):
        gold = spark.read.format("delta").load(str(root / "data/gold/integrated_taxi_trips"))
        cfg = effective_analysis_config(gold, analytics_config()["analysis"])
        register_analysis_views(spark, gold, cfg)
        query_outputs[label] = {name: spark.sql(sql_text(name)).collect() for name in QUERY_IDS}
    for name in QUERY_IDS:
        assert_same_results(query_outputs["incremental"][name], query_outputs["full"][name])
        query_checks[name] = len(query_outputs["incremental"][name])
    return {"gold": inc_fingerprint, "products": product_checks, "queries": query_checks}


def experiment_signature(updates: Path) -> str:
    digest = hashlib.sha256()
    for folder in ("src", "config", "sql"):
        for path in sorted((ROOT / folder).rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                digest.update(path.relative_to(ROOT).as_posix().encode())
                digest.update(path.read_bytes())
    digest.update(Path(__file__).read_bytes())
    digest.update((updates / "manifest.json").read_bytes())
    digest.update((BASELINE / "baseline_manifest.json").read_bytes())
    manifest = json.loads((updates / "manifest.json").read_text(encoding="utf-8"))
    for info in manifest["datasets"].values():
        file_hash = hashlib.sha256()
        with (updates / info["file"]).open("rb") as stream:
            for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                file_hash.update(block)
        if file_hash.hexdigest() != info["sha256"]:
            raise ValueError(f"Update checksum differs: {info['file']}")
        digest.update(file_hash.digest())
    return digest.hexdigest()


def run_trial(spark, base_config, updates: Path, index: int) -> dict:
    signature = experiment_signature(updates)
    trial = EVAL_ROOT / f"revised-trial-{index:02d}"
    roots = {name: trial / name for name in ("incremental", "full", "monitoring_off")}
    for root in roots.values():
        _copy_tree(BASELINE, root)
        (root / "baseline_manifest.json").unlink(missing_ok=True)
    configs = {name: _trial_config(base_config, root) for name, root in roots.items()}
    before_storage = storage_inventory(spark, roots["incremental"])
    order = ["incremental", "full", "monitoring_off"] if index % 2 else ["monitoring_off", "full", "incremental"]
    runs = {}
    for name in order:
        spark.catalog.clearCache()
        product_module.ROOT = roots[name]
        print(f"trial {index}: {name} (same baseline, full update file)", flush=True)
        runs[name] = process_updates(
            spark, configs[name], updates, f"week3-revised-{index:02d}-{name}",
            refresh_mode="full" if name == "full" else "incremental",
            monitoring_enabled=name != "monitoring_off")
        if runs[name]["status"] != "success":
            raise RuntimeError(runs[name])
    print(f"trial {index}: paired validation controls", flush=True)
    validation = validation_pair(spark, configs["incremental"], updates, UPDATE_FILES, enabled_first=bool(index % 2))
    print(f"trial {index}: verify incremental/full and monitoring on/off", flush=True)
    verification = _verify_outputs(spark, roots["incremental"], roots["full"])
    monitoring_verification = _verify_outputs(spark, roots["incremental"], roots["monitoring_off"])
    after_storage = storage_inventory(spark, roots["incremental"])
    incremental, full, disabled = (runs[n] for n in ("incremental", "full", "monitoring_off"))
    result = {
        "benchmark_version": 2, "signature": signature,
        "trial": index, "variant_order": order,
        "incremental_total_seconds": incremental["total_seconds"],
        "incremental_gold_seconds": incremental["gold"]["seconds"],
        "incremental_affected_trips": incremental["gold"]["affected_trips"],
        "incremental_products": incremental["products"],
        "full_total_seconds": full["total_seconds"],
        "full_gold_seconds": full["gold"]["seconds"],
        "full_product_seconds": sum(p["seconds"] for p in full["products"].values()),
        "validation_pair": validation,
        "validation_overhead_seconds": validation["additional_seconds"],
        "monitoring_off_total_seconds": disabled["total_seconds"],
        "monitoring_overhead_seconds": incremental["total_seconds"] - disabled["total_seconds"],
        "monitoring_write_seconds": incremental["monitoring_write_seconds"],
        "monitoring_off_write_seconds": disabled["monitoring_write_seconds"],
        "before_storage": before_storage, "after_storage": after_storage,
        "storage_delta_bytes": {key: after_storage[key] - before_storage[key]
            for key in ("total_logical_bytes", "active_business_data_bytes", "operational_and_retained_bytes")},
        "bronze_metrics": incremental["bronze"],
        "verification": verification, "monitoring_verification": monitoring_verification,
    }
    checkpoint = EVAL_ROOT / f"revised-trial-{index:02d}-result.json"
    checkpoint.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(f"trial {index}: complete", flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--updates-dir", type=Path, default=ROOT / "data/updates")
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--capture-baseline", action="store_true")
    parser.add_argument("--replace-baseline", action="store_true")
    parser.add_argument(
        "--resume", action="store_true",
        help="Reuse completed per-trial checkpoints and rerun only missing trials",
    )
    args = parser.parse_args()
    if args.capture_baseline:
        print(json.dumps(capture_baseline(args.replace_baseline), indent=2))
        return
    if not (BASELINE / "data/monitoring/published_snapshots/_delta_log").exists():
        raise FileNotFoundError("Capture a successfully published baseline first")
    if args.trials < 1:
        parser.error("--trials must be positive")

    cfg = load_config(str(ROOT / "config/datasets.yaml"))
    spark = get_spark("week3-production-evaluation")
    original_product_root = product_module.ROOT
    try:
        # Prove the production state itself has a publication before measuring clones.
        if latest_publication(spark, cfg) is None:
            raise ValueError("Production baseline has no successful publication")
        trials = []
        for index in range(1, args.trials + 1):
            checkpoint = EVAL_ROOT / f"revised-trial-{index:02d}-result.json"
            if args.resume and checkpoint.exists():
                print(f"trial {index}: reuse checkpoint", flush=True)
                record = json.loads(checkpoint.read_text(encoding="utf-8"))
                if record.get("signature") != experiment_signature(args.updates_dir.resolve()):
                    raise ValueError("Checkpoint code/config/update signature differs; rerun without --resume")
                trials.append(record)
            else:
                trials.append(run_trial(spark, cfg, args.updates_dir.resolve(), index))
        result = {
            "benchmark_version": 2,
            "method": "same baseline; matched orchestration/publication; alternating variant order; full-data monitoring on/off; materialised validation controls; no VACUUM",
            "trial_count": len(trials), "trials": trials,
        }
        output = ROOT / "artifacts/week3/evaluation.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        print(json.dumps(result, indent=2, default=str))
    finally:
        product_module.ROOT = original_product_root
        spark.stop()


if __name__ == "__main__":
    main()
