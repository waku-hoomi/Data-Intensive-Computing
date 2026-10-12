#!/usr/bin/env python3
"""Independent Python census of raw lines, baskets and every frozen rule."""
import argparse
import collections
import csv
import datetime as dt
import hashlib
import json
import math
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pyarrow.parquet as pq
from retail.common import ROOT, load_config, save_json, sha256, spark_session


def verify(storage):
    config = load_config()
    start, split, end = [dt.datetime.fromisoformat(config[k]) for k in ["discovery_start", "discovery_end", "evaluation_end"]]
    unique, removed, raw_count, window_count = set(), collections.Counter(), 0, 0
    fields = ["invoice", "stock_code", "description", "quantity", "invoice_date", "price", "customer_id", "country"]
    for path in sorted((ROOT / "data/converted").glob("*.parquet")):
        for batch in pq.ParquetFile(path).iter_batches(batch_size=20000, columns=fields):
            for row in batch.to_pylist():
                raw_count += 1
                date = row["invoice_date"]
                if date is None or not start <= date < end:
                    continue
                window_count += 1
                inv, product = row["invoice"], row["stock_code"]
                if inv and inv.upper().startswith("C"):
                    removed["cancellations"] += 1
                    continue
                if not inv or not product or any(row[k] is None or not math.isfinite(row[k]) or row[k] <= 0 for k in ["quantity", "price"]):
                    removed["invalid_sales"] += 1
                    continue
                if product.upper() in config["excluded_codes"]:
                    removed["nonproduct_entries"] += 1
                    continue
                key = tuple(row[k] for k in fields)
                if key in unique:
                    removed["exact_duplicate_lines"] += 1
                unique.add(key)
    grouped = collections.defaultdict(list)
    for row in unique:
        grouped[row[0]].append(row)
    expected, conflicts = {}, set()
    for inv, lines in grouped.items():
        if any(len({r[index] for r in lines if r[index] is not None}) > 1 for index in [4, 6, 7]):
            conflicts.add(inv)
            removed["conflicting_invoices"] += len(lines)
            continue
        expected[inv] = {"items": frozenset(r[1] for r in lines), "date": min(r[4] for r in lines)}
    quality = json.loads((ROOT / "results/quality.json").read_text())
    assert raw_count == quality["raw_rows"]
    assert window_count == quality["study_window_rows"]
    assert len(conflicts) == quality["conflicting_invoices"]
    for stage in quality["stages"]:
        assert removed[stage["stage"]] == stage["removed_rows"], stage
    assert len(expected) == quality["baskets"]
    spark = spark_session("retail-independent-verification")
    try:
        actual = {}
        for row in spark.read.parquet(storage + "/baskets").toLocalIterator():
            assert row.invoice not in actual, "Duplicate invoice in basket output"
            assert len(row.items) == len(set(row.items)) == row.basket_size
            assert row.month == row.invoice_date.strftime("%Y-%m")
            assert row.period == ("discovery" if row.invoice_date < split else "evaluation")
            actual[row.invoice] = {"items": frozenset(row.items), "date": row.invoice_date}
        assert actual == expected, "Raw-data census and Spark baskets differ"
        stored_rules = spark.read.parquet(storage + "/rules").collect()
        stored_metrics = spark.read.parquet(storage + "/metrics").collect()
    finally:
        spark.stop()
    rules = list(csv.DictReader((ROOT / "results/discovery_rules.csv").open()))
    metrics = list(csv.DictReader((ROOT / "results/temporal_metrics.csv").open()))
    endpoints = {(r["antecedent"], r["consequent"]) for r in rules}
    assert len(endpoints) == len(rules) == len(stored_rules)
    assert endpoints == {(r.antecedent, r.consequent) for r in stored_rules}
    assert len(metrics) == len(endpoints) * 4 == len(stored_metrics)
    stored = {(r.period, r.antecedent, r.consequent): r.asDict() for r in stored_metrics}
    windows = {"discovery": [r["items"] for r in expected.values() if r["date"] < split],
               "evaluation": [r["items"] for r in expected.values() if r["date"] >= split]}
    windows.update({m: [r["items"] for r in expected.values() if r["date"].strftime("%Y-%m") == m] for m in ["2011-06", "2011-07", "2011-08"]})
    counts = {}
    for period, baskets in windows.items():
        singles = collections.Counter(i for b in baskets for i in b)
        counts[period] = (len(baskets), singles, {(a, b): sum(a in basket and b in basket for basket in baskets) for a, b in endpoints})
    def compare(row, period):
        a, b = row["antecedent"], row["consequent"]
        n, singles, pairs = counts[period]
        na, nb, nab = singles[a], singles[b], pairs[a, b]
        for name, value in [("antecedent_count", na), ("consequent_count", nb), ("pair_count", nab)]:
            assert int(row[name]) == value, (period, a, b, name)
        correct = {"support": nab/n, "confidence": nab/na if na else None,
                   "lift": nab*n/(na*nb) if na and nb else None}
        for name, value in correct.items():
            if value is None:
                assert row[name] == ""
            else:
                assert math.isclose(float(row[name]), value, rel_tol=1e-10, abs_tol=1e-12)
        return na, nb, correct
    discovery_map = {}
    for row in rules:
        compare(row, "discovery")
        assert float(row["support"]) >= config["min_support"] - 1e-12
        assert float(row["confidence"]) >= config["min_confidence"]
        assert float(row["lift"]) >= config["min_lift"]
        discovery_map[row["antecedent"], row["consequent"]] = float(row["confidence"])
    for row in metrics:
        na, nb, correct = compare(row, row["period"])
        if not na or not nb:
            status = "undefined"
        elif na < config["evaluation_min_antecedent_count"] or nb < config["evaluation_min_consequent_count"]:
            status = "insufficient"
        elif correct["lift"] > 1 and correct["confidence"] / discovery_map[row["antecedent"], row["consequent"]] >= config["confidence_retention"]:
            status = "retained"
        else:
            status = "weakened"
        assert row["status"] == status
        s = stored[row["period"], row["antecedent"], row["consequent"]]
        assert s["status"] == status
        for name in ["antecedent_count", "consequent_count", "pair_count", "baskets"]:
            assert int(row[name]) == s[name]
        for name in ["support", "confidence", "lift"]:
            assert (row[name] == "" and s[name] is None) or math.isclose(float(row[name]), s[name], rel_tol=1e-10, abs_tol=1e-12)
    canonical = json.dumps([(i, str(expected[i]["date"]), sorted(expected[i]["items"])) for i in sorted(expected)], separators=(",", ":"))
    save_json(ROOT / "results/verification.json", {"passed": True, "raw_rows_checked": raw_count,
        "baskets_checked": len(expected), "discovery_rules_checked": len(rules), "evaluation_rows_checked": len(metrics),
        "basket_semantic_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
        "rules_csv_sha256": sha256(ROOT / "results/discovery_rules.csv"),
        "metrics_csv_sha256": sha256(ROOT / "results/temporal_metrics.csv"),
        "method": "Independent Python raw-line cleaning and full basket/rule census, matched to Spark HDFS outputs"})
    print("Independent full-data verification passed", flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--storage", default=str(ROOT / "data/processed"))
    verify(p.parse_args().storage)
