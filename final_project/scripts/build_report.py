#!/usr/bin/env python3
"""Two-page report and charts, populated exclusively from verified run results."""
import collections
import csv
import json
import os
import sys
from pathlib import Path
from xml.sax.saxutils import escape
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.common import ROOT
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".runtime/matplotlib"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether
from pypdf import PdfReader


def main():
    read = lambda name: json.loads((ROOT / "results" / name).read_text())
    quality, discovery, evaluation, verification = [read(n) for n in ["quality.json", "discovery.json", "evaluation.json", "verification.json"]]
    if not verification["passed"]:
        raise ValueError("Report requires successful full-data verification")
    rules = list(csv.DictReader((ROOT / "results/discovery_rules.csv").open()))
    metrics = list(csv.DictReader((ROOT / "results/temporal_metrics.csv").open()))
    lookup = {(r["period"], r["antecedent"], r["consequent"]): r for r in metrics}
    top = sorted(rules, key=lambda r: (-int(r["pair_count"]), -float(r["lift"]), r["antecedent"], r["consequent"]))[:3]
    figures = ROOT / "results/figures"
    figures.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.0), layout="constrained")
    overall = [r for r in metrics if r["period"] == "evaluation"]
    palette = {"retained": "#167b79", "weakened": "#d37735", "insufficient": "#9aa3ae", "undefined": "#656575"}
    for status in ["retained", "weakened", "insufficient"]:
        points = [r for r in overall if r["status"] == status and r["confidence"]]
        if points:
            axes[0].scatter([float(r["discovery_confidence"]) for r in points], [float(r["confidence"]) for r in points],
                            s=19, alpha=.7, color=palette[status], label=f"{status} ({len(points)})")
    axes[0].plot([0, 1], [0, 1], color="#6b7280", linestyle="--", linewidth=1)
    axes[0].set(xlabel="Discovery confidence", ylabel="June-August confidence", xlim=(0, 1.02), ylim=(0, 1.02), title="Fixed rules in subsequent orders")
    axes[0].legend(fontsize=7, loc="lower right")
    for rule in top:
        a, b = rule["antecedent"], rule["consequent"]
        values = [float(rule["confidence"])] + [float(lookup[m, a, b]["confidence"]) if lookup[m, a, b]["confidence"] else float("nan") for m in ["2011-06", "2011-07", "2011-08"]]
        axes[1].plot(range(4), values, marker="o", linewidth=1.5, label=f"{a} to {b}")
    axes[1].set(xticks=range(4), xticklabels=["Discovery", "June", "July", "August"], ylabel="Confidence", ylim=(0, 1.02), title="Three highest-count discovery rules")
    axes[1].legend(fontsize=7, loc="lower left")
    fig.savefig(figures / "rule_stability.png", dpi=220)
    plt.close(fig)
    statuses = ["retained", "weakened", "insufficient", "undefined"]
    labels = ["2011-06", "2011-07", "2011-08", "evaluation"]
    fig, ax = plt.subplots(figsize=(6, 2.8), layout="constrained")
    bottom = [0] * 4
    for status in statuses:
        values = [sum(int(r["count"]) for r in evaluation["summary"] if r["period"] == m and r["status"] == status) for m in labels]
        ax.bar(["June", "July", "August", "Combined"], values, bottom=bottom, label=status, color=palette[status])
        bottom = [a + b for a, b in zip(bottom, values)]
    ax.set(ylabel="Number of directed rules", title="Descriptive evidence status by evaluation window")
    ax.legend(fontsize=7, ncols=2)
    fig.savefig(figures / "evidence_status.png", dpi=200)
    plt.close(fig)
    reports = ROOT / "reports"
    reports.mkdir(exist_ok=True)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="ReportTitle", fontName="Helvetica-Bold", fontSize=16, leading=19, textColor=colors.HexColor("#183343"), spaceAfter=5))
    styles.add(ParagraphStyle(name="BodyReport", fontName="Helvetica", fontSize=9.4, leading=12.3, spaceAfter=6))
    styles.add(ParagraphStyle(name="SectionReport", fontName="Helvetica-Bold", fontSize=10.7, leading=14, spaceBefore=7, spaceAfter=4, textColor=colors.HexColor("#183343")))
    styles.add(ParagraphStyle(name="SmallReport", fontName="Helvetica", fontSize=8, leading=10, spaceAfter=4))
    styles.add(ParagraphStyle(name="CellReport", fontName="Helvetica", fontSize=8, leading=10))
    story = []
    def para(text, style="BodyReport"):
        story.append(Paragraph(text, styles[style]))
    def heading(text):
        para(text, "SectionReport")
    n = discovery["selected_rules"]
    combined = collections.Counter({r["status"]: r["count"] for r in evaluation["summary"] if r["period"] == "evaluation"})
    rows = {r["stage"]: r["removed_rows"] for r in quality["stages"]}
    counts = {r["month"]: r["baskets"] for r in quality["basket_summary"]}
    singletons = sum(r["singletons"] for r in quality["basket_summary"])
    para("Spark-Based Market Basket Analysis", "ReportTitle")
    para("Product association rules and their short-term stability | Group 6", "SmallReport")
    heading("1. Research question and implementation")
    para("Which product pairs show strong co-purchase associations in historical orders, and do these associations persist in the next three months? We implemented a reproducible PySpark pipeline that cleans invoice lines, constructs baskets, mines historical rules with FP-Growth, and evaluates the same rules in subsequent transactions. Product codes identify items; descriptions are display labels. Associations describe observed co-purchases and do not establish causation or increased sales.")
    para("Storage uses Parquet on a project-local HDFS (Hadoop 3.4.2, one NameNode and one DataNode, replication 1). Spark 4.0.4 executes in local[4] mode on macOS with Java 17 and Python 3.12. This demonstrates actual HDFS reads/writes, rather than multi-node scalability. Matplotlib produces the figures. Excel is converted to Parquet using openpyxl/PyArrow; subsequent analytical processing uses Spark.")
    heading("2. Data and quality controls")
    para(f"Online Retail II [1] contains {quality['raw_rows']:,} lines in two overlapping annual sheets from December 2009 to December 2011. We downloaded the official workbook and recorded SHA-256 hashes. The declared study window (December 2010-August 2011) contains {quality['study_window_rows']:,} source lines. Sequential exclusions removed {rows['cancellations']:,} cancellation lines, {rows['invalid_sales']:,} other invalid sales, {rows['nonproduct_entries']:,} non-product lines, {rows['exact_duplicate_lines']:,} exact duplicates, and {rows['conflicting_invoices']:,} lines from {quality['conflicting_invoices']:,} inconsistent invoices.")
    para(f"Valid sales require invoice/product identifiers, a valid timestamp, and finite positive quantity and price. A documented code list removes postage, charges and adjustments; legitimate alphabetic products remain eligible. Invoice timestamp, non-null customer and country values must agree. Missing customer IDs and descriptions alone are retained. The final {quality['clean_rows']:,} lines produce {quality['baskets']:,} baskets and {quality['distinct_products']:,} products. Each basket is a sorted set of distinct product codes; quantities do not weight membership. All {singletons:,} singleton baskets remain in metric denominators. Wholesale and large valid orders are retained.")
    heading("3. Discovery and fixed-rule temporal evaluation")
    para(f"Discovery uses December 1, 2010-May 31, 2011 ({discovery['baskets']:,} baskets). Evaluation uses June-August 2011 ({evaluation['evaluation_baskets']:,} baskets: June {counts['2011-06']:,}, July {counts['2011-07']:,}, August {counts['2011-08']:,}). Splits follow original calendar timestamps, represented consistently without timezone conversion. No invoice appears in both periods.")
    para("FP-Growth [2] was piloted on discovery data at 3% and 2% support. The predeclared main configuration uses support >= 2%, confidence >= 0.50, lift >= 2, at least 30 co-occurrences and 50 antecedent baskets. We retain single-product antecedents and consequents, including both directions when eligible. All selected rules and their configuration are frozen before evaluation; subsequent months are not used to retune or reselect them.")
    para("For A to B, let N be all valid baskets, nA/nB the individual occurrence counts and nAB the joint count. Support = nAB/N; confidence = nAB/nA; lift = nAB*N/(nA*nB). These counts are recomputed for the full evaluation period and each month. Missing products yield undefined metrics, while observed products with no joint purchase yield zero confidence/lift. A descriptive 'retained' label requires at least 30 occurrences of each item, lift > 1, and confidence >= 80% of discovery confidence; otherwise evidence is weakened or insufficient. The label is not a statistical significance test.")
    story.append(PageBreak())
    heading("4. Results")
    para(f"The main discovery configuration selected <b>{n} directed rules</b>. In the combined June-August window, <b>{combined['retained']} were retained</b>, {combined['weakened']} weakened, {combined['insufficient']} had insufficient observations, and {combined['undefined']} were undefined. <b>{evaluation['rules_retained_all_three_months']} rules</b> were retained in each of all three monthly windows. Monthly and combined evidence are reported separately: aggregation can hide short-lived changes.")
    story.append(Image(str(figures / "rule_stability.png"), width=505, height=189.4))
    para("Figure 1. All defined fixed-rule confidence comparisons (left); three rules chosen by discovery joint count, with lift/code tie-breaks (right). The dashed diagonal indicates unchanged confidence. No evaluation-based case selection was used.", "SmallReport")
    table_data = [["Discovery-selected product rule", "Joint count\n(discovery)", "Confidence\n(discovery / later)", "Lift\n(discovery / later)"]]
    for r in top:
        e = lookup["evaluation", r["antecedent"], r["consequent"]]
        label = f"<b>{escape(r['antecedent'])} to {escape(r['consequent'])}</b><br/>{escape(r['antecedent_name'])}<br/>to {escape(r['consequent_name'])}"
        conf = f"{float(r['confidence']):.3f} / {float(e['confidence']):.3f}" if e["confidence"] else "undefined"
        lift = f"{float(r['lift']):.2f} / {float(e['lift']):.2f}" if e["lift"] else "undefined"
        table_data.append([Paragraph(label, styles["CellReport"]), str(r["pair_count"]), conf, lift])
    table = Table(table_data, colWidths=[231, 71, 107, 96], repeatRows=1)
    table.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), colors.HexColor("#e7eef1")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"), ("FONTSIZE", (0,0), (-1,-1), 8),
        ("VALIGN", (0,0), (-1,-1), "TOP"), ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("TOPPADDING", (0,0), (-1,-1), 5), ("LINEBELOW", (0,0), (-1,0), .6, colors.HexColor("#9fb3bf")),
        ("LINEBELOW", (0,1), (-1,-1), .25, colors.HexColor("#d5dee3"))]))
    story.append(table)
    weakened = sorted([r for r in overall if r["status"] == "weakened"], key=lambda r: (r["antecedent"], r["consequent"]))
    examples = "; ".join(f"{escape(r['antecedent'])} to {escape(r['consequent'])}: {float(r['discovery_confidence']):.3f} to {float(r['confidence']):.3f}" for r in weakened[:2])
    if examples:
        para(f"The two weakened rules illustrate confidence decline ({examples}). Both still have high later lift, so lift alone would miss the decline in conditional purchase probability.")
    heading("5. Verification and interpretation limits")
    para(f"Six automated tests cover hand-calculated metrics, absent products, zero co-occurrence, low observation counts, cleaning and date boundaries, and FP-Growth frequencies. An independent Python census re-cleans all {verification['raw_rows_checked']:,} source rows and matches all {verification['baskets_checked']:,} Spark baskets, {verification['discovery_rules_checked']} discovery rules and {verification['evaluation_rows_checked']} evaluation rows, including HDFS-to-CSV agreement. Input hashes, fixed configuration and semantic output hashes support reproduction.")
    para("Results are conditional on this retailer, cleaning policy and support cutoff. Strong rules may represent coordinated giftware ranges or bulk purchasing. Seasonal changes and disappearing products can affect stability; three months cannot establish long-term persistence. The rule labels use explicit practical thresholds and do not quantify sampling uncertainty. Rare useful combinations below the discovery cutoff are outside the study.")
    heading("6. Run instructions and references")
    para("From <b>final_project/</b>, install Python 3.12+, then run <font name='Courier'>bash scripts/setup.sh</font> and <font name='Courier'>bash scripts/run_all.sh</font>. Setup obtains Java 17 (if needed), dependencies, Hadoop and official data. The full command starts/stops HDFS and regenerates cleaning, rules, evaluation, tests and this report. See README.md for prerequisites, individual stages and the optional local-storage mode. Raw data and runtimes are downloaded, not committed; full tables and figures are in results/.", "SmallReport")
    para("[1] Chen, D. (2012). <i>Online Retail II</i> [Data set]. UCI Machine Learning Repository. https://doi.org/10.24432/C5CG6D<br/>[2] Apache Spark 4.0.4. <i>Frequent Pattern Mining</i>. https://spark.apache.org/docs/4.0.4/ml-frequent-pattern-mining.html", "SmallReport")
    def footer(canvas, doc):
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#62717b"))
        canvas.drawString(45, 25, "Group 6 | ID2221 Final Project")
        canvas.drawRightString(A4[0]-45, 25, f"{doc.page} / 2")
    target = reports / "Final_Project_Report.pdf"
    SimpleDocTemplate(str(target), pagesize=A4, leftMargin=45, rightMargin=45,
                      topMargin=35, bottomMargin=40, title="Market Basket Analysis and Short-Term Rule Stability", author="Group 6").build(story, onFirstPage=footer, onLaterPages=footer)
    pages = len(PdfReader(target).pages)
    if pages != 2:
        raise ValueError(f"Expected exactly two report pages; generated {pages}")
    print("Generated verified two-page report", flush=True)


if __name__ == "__main__":
    main()
