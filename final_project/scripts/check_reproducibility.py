#!/usr/bin/env python3
"""Compare deterministic analytical outputs of two complete runs."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.common import ROOT, save_json


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--baseline", type=Path, required=True)
    args = p.parse_args()
    before = json.loads(args.baseline.read_text())
    after = json.loads((ROOT / "results/verification.json").read_text())
    keys = ["raw_rows_checked", "baskets_checked", "discovery_rules_checked", "evaluation_rows_checked",
            "basket_semantic_sha256", "rules_csv_sha256", "metrics_csv_sha256"]
    differences = {k: {"first": before[k], "second": after[k]} for k in keys if before[k] != after[k]}
    save_json(ROOT / "results/reproducibility.json", {"passed": not differences, "compared": keys,
        "differences": differences, "first_run": {k: before[k] for k in keys}, "second_run": {k: after[k] for k in keys},
        "scope": "Two complete runs from the same original workbook; timings and physical storage IDs are excluded"})
    if differences:
        raise SystemExit("Reproducibility mismatch; inspect results/reproducibility.json")
    print("Two complete runs have identical baskets, rules and temporal metrics")
