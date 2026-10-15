#!/usr/bin/env python3
"""Plot verified analysis tables. This command never creates or edits reports."""
import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.common import ROOT, sha256

os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".runtime/matplotlib"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    results = ROOT / "results"
    verification = json.loads((results / "verification.json").read_text())
    if not verification["passed"]:
        raise ValueError("Visualization requires successful full-data verification")
    for name, key in [("discovery_rules.csv", "rules_csv_sha256"),
                      ("temporal_metrics.csv", "metrics_csv_sha256")]:
        if sha256(results / name) != verification[key]:
            raise ValueError(f"{name} changed after verification; verify results again")
    evaluation = json.loads((results / "evaluation.json").read_text())
    with (results / "discovery_rules.csv").open() as handle:
        rules = list(csv.DictReader(handle))
    with (results / "temporal_metrics.csv").open() as handle:
        metrics = list(csv.DictReader(handle))
    lookup = {(r["period"], r["antecedent"], r["consequent"]): r for r in metrics}
    top = sorted(rules, key=lambda r: (-int(r["pair_count"]), -float(r["lift"]),
                                      r["antecedent"], r["consequent"]))[:3]
    figures = results / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.0), layout="constrained")
    overall = [r for r in metrics if r["period"] == "evaluation"]
    palette = {"retained": "#167b79", "weakened": "#d37735",
               "insufficient": "#9aa3ae", "undefined": "#656575"}
    for status in ["retained", "weakened", "insufficient"]:
        points = [r for r in overall if r["status"] == status and r["confidence"] != ""]
        if points:
            axes[0].scatter([float(r["discovery_confidence"]) for r in points],
                            [float(r["confidence"]) for r in points], s=19, alpha=.7,
                            color=palette[status], label=f"{status} ({len(points)})")
    axes[0].plot([0, 1], [0, 1], color="#6b7280", linestyle="--", linewidth=1)
    axes[0].set(xlabel="Discovery confidence", ylabel="June-August confidence",
                xlim=(0, 1.02), ylim=(0, 1.02), title="Fixed rules in subsequent orders")
    axes[0].legend(fontsize=7, loc="lower right")
    for rule in top:
        a, b = rule["antecedent"], rule["consequent"]
        values = [float(rule["confidence"])]
        for month in ["2011-06", "2011-07", "2011-08"]:
            value = lookup[month, a, b]["confidence"]
            values.append(float(value) if value != "" else float("nan"))
        axes[1].plot(range(4), values, marker="o", linewidth=1.5, label=f"{a} to {b}")
    axes[1].set(xticks=range(4), xticklabels=["Discovery", "June", "July", "August"],
                ylabel="Confidence", ylim=(0, 1.02), title="Three highest-count discovery rules")
    axes[1].legend(fontsize=7, loc="lower left")
    fig.savefig(figures / "rule_stability.png", dpi=220)
    plt.close(fig)

    labels = ["2011-06", "2011-07", "2011-08", "evaluation"]
    fig, ax = plt.subplots(figsize=(6, 2.8), layout="constrained")
    bottom = [0] * 4
    for status in ["retained", "weakened", "insufficient", "undefined"]:
        values = [sum(int(r["count"]) for r in evaluation["summary"]
                      if r["period"] == month and r["status"] == status) for month in labels]
        ax.bar(["June", "July", "August", "Combined"], values, bottom=bottom,
               label=status, color=palette[status])
        bottom = [a + b for a, b in zip(bottom, values)]
    ax.set(ylabel="Number of directed rules", title="Descriptive evidence status by evaluation window")
    ax.legend(fontsize=7, ncols=2)
    fig.savefig(figures / "evidence_status.png", dpi=200)
    plt.close(fig)
    print(f"Saved {len(rules)} discovery rules and {len(metrics)} evaluation rows in results/")
    print("Charts: results/figures/rule_stability.png and results/figures/evidence_status.png")


if __name__ == "__main__":
    main()
