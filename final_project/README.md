# Online Retail Market Basket Analysis

Group 6, ID2221 Data-Intensive Computing. This final project implements the approved proposal: **Spark-Based Market Basket Analysis: Product Association Rules and Their Short-Term Stability**.

Discover directed product-pair rules in December 2010-May 2011, freeze them, and evaluate the same rules on June-August 2011 orders, both monthly and combined. The implementation does not tune parameters on the evaluation period or claim a causal sales effect.

## Quick start

Run these commands from `final_project/` in the repository:

```bash
bash scripts/setup.sh
bash scripts/run_all.sh
```

Prerequisites: macOS or Linux, Python 3.12 or later, Bash, `curl`, `tar`, internet access for initial downloads, about 5 GB free disk and 8 GB available RAM. Tested on Apple Silicon macOS, Python 3.12.14 and Java 17. Use `RETAIL_PYTHON=/path/to/python3.12 bash scripts/setup.sh` to select an interpreter. Python 3.12 is the verified version.

Setup creates `.venv/`, installs pinned dependencies (including constraints from `results/environment_lock.txt`), downloads Java 17 if `JAVA_HOME` and a project-local Java installation are unavailable, downloads verified Apache Hadoop 3.4.2, and obtains the official UCI dataset. Supply `JAVA_HOME` pointing to a Java 17 JDK to use your own installation. The project retains its own Java runtime and does not depend on a Lab1/Lab4 directory.

`run_all.sh` starts a dedicated HDFS NameNode and DataNode, converts the original workbook, runs Spark cleaning/mining/evaluation, independently verifies all results, runs tests, records HDFS evidence, generates the two-page report, and stops the daemons on exit. It runs each Spark stage in a separate driver process. Logs are in `.runtime/logs/`; a failed stage prints its recent log and stops the pipeline.

No SSH server, Docker, YARN or system-wide Hadoop installation is needed. HDFS RPC uses `127.0.0.1:19000`; its NameNode interface uses `127.0.0.1:19870`, and DataNode ports are 19864/19866/19867. These ports must be available. Metadata and blocks stay in `.runtime/hdfs/`. The HDFS launcher makes temporary symlinks to support project directory names containing spaces. This is a single-machine HDFS demonstration with replication 1 and Spark `local[4]`, not a multi-node scalability benchmark.

For a complete alternative run on local storage:

```bash
bash scripts/run_all.sh --local
```

This produces the same analytical outputs under `data/processed/`. The submitted measured run uses HDFS. Local mode does not replace the HDFS evidence and does not require the Hadoop daemons.

## Inputs and provenance

- Dataset: Chen, D. (2012). *Online Retail II* [Data set]. UCI Machine Learning Repository. https://doi.org/10.24432/C5CG6D
- Official information: https://archive.ics.uci.edu/dataset/502/online%2Bretail%2Bii
- Official ZIP: https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip
- Expected ZIP SHA-256: `572e36277c2390fbfde10664750731e0a86f55e33470d91919085f0408e67bfb`
- Workbook SHA-256: `bcbe73b35f5b7babf197fb0cb983a11f5d9ff929078d4aa53d171b1f2df2e980`

The original ZIP belongs at `data/raw/online_retail_ii.zip`. It contains one workbook, `online_retail_II.xlsx`, with 525,461 and 541,910 lines in overlapping annual sheets (1,067,371 total). `results/source_manifest.json` records input and converted-file hashes. The ZIP and workbook are not committed. Ingestion validates the workbook hash against the recorded source and stops on unexpected changes. To ingest a supplied ZIP containing that same workbook, run `.venv/bin/python scripts/project.py ingest --source /path/to/file.zip`.

Conversion reads Excel in streaming mode using openpyxl, preserves all rows and sheet/row provenance, and writes typed Parquet with PyArrow. Analytical cleaning and aggregation use Spark. Invoice and product codes are strings; quantities/prices are numeric; timestamps preserve the dataset's original calendar date/time. Python and Spark use UTC consistently to avoid host-timezone shifts of these timezone-naive timestamps; this is a representation convention, not evidence of UTC measurement in the source.

## Cleaning and basket definition

`retail/clean.py` and `config/project.json` define sequential exclusions:

1. Keep the declared study window `[2010-12-01, 2011-09-01)` by transaction timestamp.
2. Remove cancellation invoices starting with `C`, case-insensitively.
3. Require invoice/product identifiers, a date, and finite positive quantity and price.
4. Exclude the explicit non-product codes `POST`, `DOT`, `M`, `BANK CHARGES`, `ADJUST`, `D`, `AMAZONFEE`, `CRUK`, `C2`, `S`, `B`, case-insensitively. These represent postage, manual/discount/adjustment, fee, sample or bad-debt entries. `results/quality.json` shows descriptions and counts actually encountered. Alphabetic product codes such as `PADS` are retained.
5. Deduplicate identical values across all eight original columns. This removes duplicates across and within sheets; presence-only baskets do not depend on repeated identical lines.
6. Exclude whole invoices with conflicting timestamps, non-null customer IDs or countries after the above checks. Missing customer IDs alone remain eligible; missing descriptions do not remove a product.

An invoice forms one sorted, distinct product set. Repeated products and quantities do not weight membership. Nonempty singleton baskets remain in all denominators. Large and wholesale baskets remain; no truncation or country/customer filtering is applied. The quality report gives disjoint exclusion counts, monthly baskets/singletons, basket-size quantiles, and the largest basket.

Discovery is `[2010-12-01, 2011-06-01)`; evaluation is `[2011-06-01, 2011-09-01)`. Splitting uses transaction dates, not workbook names. Conflict checks ensure an invoice cannot cross periods.

## Rule discovery and evaluation

`retail/mining.py` uses Spark ML's `FPGrowth`. Two discovery-only pilots use 3% and 2% minimum support. The predeclared main configuration is 2% support, 0.5 confidence, lift >= 2, at least 30 joint baskets and at least 50 antecedent baskets. After mining, retain only one-product antecedents and one-product consequents. Spark can mine longer itemsets internally; the support cutoff controls their number. Do not set extremely low support without checking memory and itemset counts. The 2% configuration produced 398 frequent itemsets in the recorded run.

All eligible directed rules are saved, not just a displayed top-k. Descriptions are selected from discovery data only: most frequent description, lexical tie-break. The rule table/config/input provenance are frozen in `results/discovery.json`; evaluation rejects changed configuration, rule CSV or quality-file hashes.

For each evaluation window, let `N` be all valid baskets, `nA`/`nB` the number containing each product, and `nAB` the number containing both:

```text
support(A -> B)    = nAB / N
confidence(A -> B) = nAB / nA
lift(A -> B)       = nAB * N / (nA * nB)
```

All frozen rules survive evaluation via left joins. If `nA=0`, confidence is undefined (blank CSV/null Parquet); if either product is absent, lift is undefined. If both products are observed but never together, confidence and lift are zero.

The descriptive labels were fixed before evaluation:

- **Undefined:** either item is absent.
- **Insufficient:** both are present, but either has fewer than 30 baskets.
- **Retained:** sufficient observations, lift > 1, and confidence at least 80% of discovery confidence.
- **Weakened:** sufficient observations but the retained condition fails.

These are practical descriptive thresholds, not hypothesis tests or uncertainty intervals. The combined window and all three months are assessed separately. The plotted examples are chosen by descending discovery joint count, descending discovery lift, then product codes. Actual associations may reflect giftware sets, seasonality and wholesale ordering; they do not prove recommendation effectiveness.

## Outputs and recorded results

| Output | Purpose |
|---|---|
| `reports/Final_Project_Report.pdf` | Exactly two pages: problem, data, methods, results, limitations and run instructions |
| `results/source_manifest.json` | Official source and file hashes |
| `results/quality.json` | Cleaning exclusions and basket summaries |
| `results/discovery.json` | Discovery-only pilots and frozen parameters/provenance |
| `results/discovery_rules.csv` | All selected directed rules and discovery metrics |
| `results/temporal_metrics.csv` | All rules in each month and the combined evaluation window |
| `results/evaluation.json` | Evidence-status counts and number retained in all months |
| `results/verification.json` | Independent full-data census and semantic hashes |
| `results/reproducibility.json` | Comparison of two complete runs |
| `results/tests.xml` | Automated test results |
| `results/hdfs_evidence.json` | DataNode report, actual HDFS file listing and filesystem integrity |
| `results/figures/` | Rule comparison, monthly trends and evidence-status charts |
| `results/run_*.json` | Measured stage duration and environment (single-run timings, not benchmarks) |

The measured run produced **306,691 cleaned lines**, **12,346 baskets** and **3,619 products**. There are **8,034 discovery baskets** and **4,312 evaluation baskets**. The final **52 directed rules** include **50 retained** and **2 weakened** in the combined evaluation window. **39 rules are retained in every one of the three monthly windows**. A combined-window label therefore does not guarantee consistency in each month.

Processed Parquet tables are stored in HDFS at `hdfs://127.0.0.1:19000/retail_project/` (`clean_lines`, `conflicts`, `baskets`, `frequent_itemsets`, `rules`, `metrics`). The model is saved locally at `models/fpgrowth/`. Both can be rebuilt. Raw data, models, HDFS blocks, runtimes and virtual environments are excluded from Git.

## Individual stages and validation

Keep HDFS running in the same terminal while running stages:

```bash
.venv/bin/python scripts/hdfs.py start
.venv/bin/python scripts/project.py ingest
.venv/bin/python scripts/project.py prepare --storage hdfs://127.0.0.1:19000/retail_project
.venv/bin/python scripts/project.py discover --storage hdfs://127.0.0.1:19000/retail_project
.venv/bin/python scripts/project.py evaluate --storage hdfs://127.0.0.1:19000/retail_project
.venv/bin/python scripts/verify_results.py --storage hdfs://127.0.0.1:19000/retail_project
.venv/bin/python -m pytest -q tests
.venv/bin/python scripts/hdfs.py snapshot
.venv/bin/python scripts/build_report.py
.venv/bin/python scripts/hdfs.py stop
```

Six tests cover hand-counted metrics (including singleton denominators), absent products, zero co-occurrence, insufficient counts, cleaning/time boundaries, identifier normalization, and FP-Growth frequencies/directions. The full-data verifier is independent of Spark's cleaning and metric formulas: Python re-cleans every original row, reconstructs all baskets, counts every selected product pair in every period, and matches CSV against persisted Parquet. It also checks selection cutoffs and evidence labels. `verification.json` records the basket semantic hash and sorted rule/metric CSV hashes.

Reruns overwrite this project's generated outputs intentionally. Preserve a copy of `results/` if experimenting with new parameters. If changing cleaning, dates or rule thresholds, rebuild `prepare`, `discover` and `evaluate` in that order. A new experiment must be identified separately; do not choose thresholds by inspecting the evaluation scores.

To independently compare two runs with unchanged configuration, save a baseline before rerunning:

```bash
cp results/verification.json .runtime/baseline_verification.json
bash scripts/run_all.sh
.venv/bin/python scripts/check_reproducibility.py --baseline .runtime/baseline_verification.json
```

This checks all basket memberships/timestamps and the complete rule/metric CSV hashes, excluding durations and physical HDFS block identifiers. The delivered `results/reproducibility.json` records two matching full HDFS runs.

For report visual QA, run `pdftoppm -png reports/Final_Project_Report.pdf /tmp/retail-report-page` if Poppler is installed. Report generation asserts exactly two pages. The delivered PDF was rendered and visually reviewed.

## References

- Spark FP-Growth: https://spark.apache.org/docs/4.0.4/ml-frequent-pattern-mining.html
- Hadoop single-node setup: https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/SingleCluster.html
- Approved proposal: `docs/Approved_Project_Proposal.pdf`
