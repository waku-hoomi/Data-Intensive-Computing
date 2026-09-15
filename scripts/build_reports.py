"""Build the two submission PDFs and editable Markdown from measured evidence.

Requires reportlab (report-only dependency); never substitutes invented results.
"""
from pathlib import Path
import json
import math
import statistics
import html
import reportlab
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak

ROOT=Path(__file__).resolve().parents[1]
ART=ROOT/"artifacts"
OUT=ROOT/"reports"
OUT.mkdir(exist_ok=True)
FONTDIR=Path(reportlab.__file__).parent/"fonts"
pdfmetrics.registerFont(TTFont("Body",str(FONTDIR/"Vera.ttf")))
pdfmetrics.registerFont(TTFont("BodyBold",str(FONTDIR/"VeraBd.ttf")))
BLUE=colors.HexColor("#18334D")
TEAL=colors.HexColor("#176B70")
STYLES={
 "title":ParagraphStyle("title",fontName="BodyBold",fontSize=22,leading=27,textColor=BLUE,spaceAfter=15),
 "h":ParagraphStyle("h",fontName="BodyBold",fontSize=14,leading=19,textColor=TEAL,spaceBefore=11,spaceAfter=8),
 "p":ParagraphStyle("p",fontName="Body",fontSize=10,leading=14.2,spaceAfter=9),
 "small":ParagraphStyle("small",fontName="Body",fontSize=8.2,leading=11.4,spaceAfter=7),
 "cell":ParagraphStyle("cell",fontName="Body",fontSize=8.4,leading=11.2),
 "head":ParagraphStyle("head",fontName="BodyBold",fontSize=8.4,leading=11.2,textColor=colors.white),
}


def read(name):
    return json.loads((ART/name).read_text())


bench={r["name"]:r for r in read("benchmark_all.json")}
assert len(bench)==10 and all(r["results_equal"] for r in bench.values())
products=read("products.json")
valid=read("final_validation.json")
env=read("benchmark_environment.json")
months=read("query_results/06_monthly_trend.json")
aq=read("query_results/03_air_quality_demand.json")
q4=sorted(read("query_results/04_weather_variation.json"),key=lambda r:r["variation_rank"])
total_product_bytes=sum(p["storage_bytes"] for p in products)
total_build=sum(p["build_seconds"] for p in products)


def esc(text):
    return html.escape(str(text)).replace("\n","<br/>")


def P(text,style="p"):
    return Paragraph(esc(text),STYLES[style])


def table(rows,widths):
    data=[[P(v,"head" if i==0 else "cell") for v in row] for i,row in enumerate(rows)]
    t=Table(data,colWidths=widths,repeatRows=1,hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),BLUE),("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(-1,-1),7),("RIGHTPADDING",(0,0),(-1,-1),7),
        ("TOPPADDING",(0,0),(-1,-1),7),("BOTTOMPADDING",(0,0),(-1,-1),7),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.HexColor("#F0F5F7"),colors.white]),
        ("LINEBELOW",(0,0),(-1,0),.5,BLUE),
        ("LINEBELOW",(0,1),(-1,-1),.35,colors.HexColor("#D6E0E5")),
    ]))
    return t


def render(name,label,pages):
    story=[]
    markdown=[f"# {label}\n"]
    for page_no,blocks in enumerate(pages):
        if page_no:
            story.append(PageBreak())
            markdown.append("\n---\n")
        for block in blocks:
            kind=block[0]
            if kind=="table":
                rows,widths=block[1:]
                story.extend([table(rows,widths),Spacer(1,10)])
                markdown.extend(["| "+" | ".join(map(str,rows[0]))+" |",
                                 "| "+" | ".join(["---"]*len(rows[0]))+" |"])
                markdown.extend("| "+" | ".join(map(str,r))+" |" for r in rows[1:])
                markdown.append("")
            else:
                story.append(P(block[1],kind))
                markdown.extend([("## " if kind=="h" else "" )+block[1],""])
    def footer(canvas,doc):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#D6E0E5"))
        canvas.line(48,40,A4[0]-48,40)
        canvas.setFont("Body",8)
        canvas.setFillColor(BLUE)
        canvas.drawString(48,27,"ID2221 | Lab 2 | "+label)
        canvas.drawRightString(A4[0]-48,27,str(doc.page))
        canvas.restoreState()
    SimpleDocTemplate(str(OUT/f"{name}.pdf"),pagesize=A4,leftMargin=48,rightMargin=48,
                      topMargin=43,bottomMargin=52,title=label,author="ID2221 Lab 2 group").build(
                          story,onFirstPage=footer,onLaterPages=footer)
    (ROOT/"docs"/f"{name}.md").write_text("\n".join(markdown))


design=[
[("title","Urban Analytics Platform"),
 ("p","Lab 2 Design Report | ID2221 Data-Intensive Computing | 15 September 2026"),
 ("h","1. Analytical requirements and data foundation"),
 ("p","This extension builds reusable Spark SQL analyses and four materialized Delta products on the group's Lab 1 platform. It answers six mobility/environment questions, investigates caching, partition pruning, broadcast joins and Adaptive Query Execution (AQE), and verifies that optimization preserves results. Entry points and SQL are separate so analysts can repeat a query or refresh products without editing pipeline internals."),
 ("p",f"The six checksum-verified course inputs contain three Yellow Taxi Parquet files for January-March 2024, weather CSV, EPA PM2.5 CSV and the taxi-zone lookup. Corrected ingestion retains 9,417,864 unique trips, 7,928,224 air-quality measurements, 8,784 weather observations and 265 zones. Full Gold has exactly one row per accepted trip. Analysis uses {valid['scope']['rows']:,} trips within local 2024-01-01 inclusive to 2024-04-01 exclusive; 20 accepted trips fall outside this explicit interval."),
 ("h","Corrections inherited by every analysis"),
 ("p","NYC air quality is selected with New York state code 36 and NYC county codes before aggregation. California/Kings must never become Brooklyn. Multiple instruments are averaged within a station-hour, then distinct stations receive equal weight within a borough-hour. Full-data geographic invariance was checked by removing all out-of-state input: NYC aggregates did not change."),
 ("p","Taxi IDs hash sorted named JSON fields, preserving NULL positions. A candidate vendor/time/location key still has 348 duplicate groups in cleaned data, so it is not a reliable key. Schema checks now cover columns used by transformations; invalid parses tolerate ANSI mode; deterministic duplicate handling prefers valid rows. An additional 180 negative-duration trips were rejected. These changes require regenerated Lab 1 tables; old and new ID schemes must not be mixed."),
 ("h","Time and missing-data policy"),
 ("p","Storage timestamps remain UTC; pickup demand is reported in America/New_York local time. The course weather CSV does not specify its timezone. We retain Lab 1's UTC interpretation as an explicitly approved but unverified assumption. No sensitivity experiment was performed. If its timestamps actually represent NYC local time, weather associations may shift by 4-5 hours. Numerical weather codes remain categories without invented labels. Missing environmental context remains NULL, not zero."),
 ("small","Scope and limitation: analysis describes accepted Yellow Taxi records in one quarter, not every NYC journey. Association is not causation. Source provenance and cleaning decisions constrain interpretation even when SQL and optimization checks pass.")],
[("h","2. Reusable Spark SQL query design"),
 ("p","All six analyses live in sql/queries/. The common analysis_trips view applies the configured UTC bounds corresponding to the local reporting interval and exposes local date, month, pickup hour and duration. SQL views organize shared calculations; the source and products remain Delta tables."),
 ("table",[
 ["Query","Definition and output"],
 ["Q1 Zone monthly demand","Count accepted pickups by local month, pickup-zone ID, zone and borough. Retain unknown geography as a separate NULL group."],
 ["Q2 Weather and distance","Group by source weather code. Return trip count, valid distance count and average trip distance in TLC miles."],
 ["Q3 PM2.5 and demand","One pair per borough-hour with available PM2.5: measurement, trip count. Return sample size, mean PM2.5, mean demand and Pearson correlation."],
 ["Q4 Weather demand variation","For each zone/code calculate trips per observed hour. Require 24 hours per code and two eligible codes per zone; rank max-minus-min hourly rates."],
 ["Q5 Weekly peak hours","Group demand by ISO weekday and local hour; divide by actual observed elapsed hours. Preserve tied peak hours with DENSE_RANK."],
 ["Q6 Monthly trend","Report monthly trips, calendar days, daily mean and month-over-month percentage. The first month has no prior-quarter comparison."],
 ],[123,376]),
 ("h","Correct denominators and comparison units"),
 ("p",f"An explicit UTC hour spine contains {valid['elapsed_hours']:,} elapsed hours in the 91-day local interval, including the March DST transition. Crossing observed zone IDs with these hours retains zero-trip zone-hours. This prevents rare weather from appearing to lower demand solely because it occurred for fewer hours. Q4 compares category-specific means; it is not a causal model and does not adjust for weekday, season or time of day."),
 ("p","Q3 avoids trip-weighting a repeated hourly PM2.5 value. Safe covariance divided by the product of standard deviations returns NULL when variance is zero. Borough-hours without inherited air-quality context are excluded from correlation and remain flagged missing in the product. Because context comes from the integrated trips, a zero-trip borough-hour cannot recover a sensor observation from that table alone; the reported association is conditional on available context."),
 ("p","Q5 uses actual hourly observations so an absent spring-forward hour is not counted as an observed zero. Q6 uses calendar days, not only days on which a trip was observed. The same definitions are used before and after optimization.")],
[("h","3. Materialized analytical data products"),
 ("p","The four products are generated automatically from a version-pinned integrated Delta table. All are small enough to remain unpartitioned in this run. Materialization moves repeated aggregation to refresh time, while keeping storage and freshness costs explicit."),
 ("table",[
 ["Product / grain","Consumer and materialization rationale"],
 ["Daily mobility / local date + borough","Operations and planning analysts inspect daily volumes and monthly trends. Store counts plus sums and valid counts for distance, duration and fare, enabling correctly weighted roll-ups."],
 ["Taxi-zone statistics / month + zone","Transport planners compare zones without scanning individual journeys. Store counts and mean distance, duration and fare; Q1 reads the prepared monthly counts."],
 ["Weather impact / zone + weather code","Service planners compare rates and trip distances by observed condition code. Store observed hours, trips, distance sums/counts and mean hourly demand, reusing the costly zero-hour scaffolding."],
 ["Air-quality impact / borough + hour","Environmental analysts reuse demand, PM2.5, station count and availability flags. One stored observation per borough-hour supports Q3 without repeatedly regrouping trips."],
 ],[164,335]),
 ("h","Metadata and refresh contract"),
 ("p","Each product is a Delta table under data/products/. An append-only Delta registry under data/lab2_metadata/product_registry records product name, source path and Delta version, initial creation time, refresh time, schema version and schema JSON, analysis configuration, row count, active-file count, storage bytes and build duration. Refresh overwrites a product and appends a registry record; initial creation time is retained."),
 ("p","Products in this experiment use Gold version 0. The benchmark refuses product metadata referring to another Gold version or analysis configuration. Refreshes run serially in this local batch workflow. Publication across the product table and registry is not a multi-table transaction; simultaneous refresh/benchmark execution is outside the supported workflow. A production service should publish a manifest only after all tables are ready."),
 ("p",f"The four products total {total_product_bytes:,} active Parquet bytes ({total_product_bytes/1024**2:.3f} MiB), excluding Delta logs. Their counts and sums were validated against source scope. The air product covers {valid['scope']['rows_with_borough']:,} trips with a non-null borough; {valid['scope']['rows']-valid['scope']['rows_with_borough']:,} trips with missing borough cannot receive a borough-hour group. Daily, zone and weather products preserve the full scoped trip count."),
 ("small","Creation/refresh timestamps in metadata are ISO-8601 UTC strings. Large raw inputs and generated Delta tables are reproducible outputs and are not required inside the source submission ZIP.")],
[("h","4. Optimization strategy and evidence"),
 ("p","Each technique is investigated independently with result equality checks. A fixed baseline disables AQE and automatic broadcast joins, retaining eight shuffle partitions. This is an explicit experimental baseline, not a claim that Spark's defaults always behave this way. All results are local warm-process measurements; file-system caches are not flushed."),
 ("table",[
 ["Technique","Selected experiment / expected plan evidence"],
 ["Caching","Q2 on analysis_trips, uncached versus explicitly materialized SQL cache. Expect an in-memory scan. Cache setup is measured separately and must be amortized."],
 ["Partition pruning","Q1 restricted to February in both variants. Add pickup_year=2024 and pickup_month IN (2,3). UTC March is required for February's final local evening. Expect PartitionFilters."],
 ["Broadcast join","Read underlying Bronze taxi trips and join the 265-row zone lookup. Compare MERGE with BROADCAST(z), holding scope fixed. Expect BroadcastHashJoin BuildRight."],
 ["AQE","Run Q1 with identical SQL, eight initial shuffle partitions and broadcasting disabled; switch AQE only. Check final adaptive plan and coalesced shuffle readers."],
 ],[113,386]),
 ("p","Final query comparisons use materialized zone statistics for Q1, explicit analysis cache for Q2, the hourly air product for Q3, the weather summary for Q4, cached city-hours for Q5, and daily mobility roll-ups for Q6. Each retains the baseline result schema and semantics. No technique is claimed to apply usefully to every query."),
 ("p","Trade-offs differ by technique. Explicit partition predicates couple a query to the physical layout and require careful local/UTC boundaries. Broadcast replicates the small side in executor memory, so it must be bounded by measured table size. AQE adds runtime planning work and can change parallelism between executions. Cache consumes memory or spills to disk, has initialization cost, and must be invalidated or refreshed when inputs change."),
 ("h","Measurement discipline"),
 ("p","Each variant has one warm-up immediately before each of three timed collect() executions. Pair order alternates before/after, after/before, before/after. Spark caches are cleared between variants; cache setup is outside query latency but retained separately. EXPLAIN FORMATTED and the executed plan after collection are saved, including the final AQE plan. The three timings, result rows and equality outcome remain available in evidence."),
 ("p","Comparison ignores output ordering, requires exact integer/null agreement and uses relative tolerance 1e-9 plus absolute tolerance 1e-8 for floating-point aggregates. Both repeated executions and original/optimized outputs are checked. Nine automated tests cover manually calculated SQL, zero-demand hours, DST, zero variance, corrected geography, identity, schema parsing, Delta refresh and benchmark artifacts."),
 ("small","The benchmark report separates speedups among the four required techniques from the larger gains achieved by precomputing products; it also reports product build time, storage and cache setup cost.")],
[("h","5. Findings, limitations and engineering trade-offs"),
 ("p",f"Demand increases from {months[0]['trip_count']:,} trips in January to {months[2]['trip_count']:,} in March. Calendar-normalized daily means rise from {months[0]['mean_daily_trips']:,.0f} to {months[2]['mean_daily_trips']:,.0f}. Weekday peaks are 18:00; Saturday peaks at 19:00 and Sunday at 00:00 under the configured local-time definition. These describe this quarter of cleaned Yellow Taxi data."),
 ("p",f"PM2.5-demand Pearson correlations are {next(x['pearson_r'] for x in aq if x['pickup_borough']=='Brooklyn'):.3f} for Brooklyn, {next(x['pearson_r'] for x in aq if x['pickup_borough']=='Queens'):.3f} for Queens and {next(x['pearson_r'] for x in aq if x['pickup_borough']=='Bronx'):.3f} for Bronx. They are weak unadjusted linear associations, not evidence that pollution changes demand. Q4 ranks {q4[0]['pickup_zone']} first, with an hourly-rate range of {q4[0]['absolute_variation']:.2f} trips across eligible weather codes. Time-of-day and season confounding remain."),
 ("p","Air quality is missing for roughly 90% of scoped trips, largely reflecting uneven geographic coverage. Weather matches every scoped trip under the UTC assumption, but a high matching rate cannot validate the timezone. Weather category meanings, precise station provenance and timezone were not supplied; descriptive weather labels and causal conclusions are deliberately avoided."),
 ("h","Which trade-offs are justified here?"),
 ("p","Small unpartitioned summaries provide strong repeated-query savings with very little storage overhead. Cached full analysis data is fast to reuse but expensive to initialize, so it should serve repeated workloads rather than a single one-off query. Borough-hour or zone-weather aggregation remains the heavier baseline work because it must scan/group trips and account for absent observations. Broadcast is appropriate for the tiny lookup, not the nationwide air-quality fact table. With only eight initial shuffle partitions, AQE has little excess parallelism to remove."),
 ("h","Expansion to ten cities and handoff"),
 ("p","Add city_id to every entity, key and aggregation grain; maintain per-city timezone and provenance configuration instead of extending NYC-specific assumptions. Monitor skew, file sizes and join-side size before selecting city/date partitioning or broadcast. Replace unconditional overwrite with source-version-aware incremental maintenance where justified; publish product versions and freshness metadata together. Avoid broadcasting an environmental table merely because it was small for one city."),
 ("p","Lab 3 should start from the regenerated Lab 2 baseline and keep the documented UTC weather interpretation. No alternative production dataset exists. Preserve weighted sums/counts when extending roll-ups, refresh products when Gold or analysis scope changes, and rerun key/row-count checks after ingestion changes. The current workflow is batch refresh, not an already implemented incremental platform."),
 ("small","Reproducibility: README, config/input_manifest.json, SQL files, source and tests accompany the reports. Final validation, source/product metadata, query outputs, all timing trials and physical plans are included as evidence. See docs/LAB3_HANDOFF_ZH.md for the concise group handoff.")],
]

tech_names=[("technique_cache","Cache (Q2)"),("technique_partition_pruning","Pruning (February Q1)"),
            ("technique_broadcast","Broadcast zone join"),("technique_aqe","AQE (Q1)")]
tech_rows=[["Experiment","Before (s)","After (s)","Speedup"]]+[
    [label,f"{bench[k]['median_seconds']['before']:.4f}",f"{bench[k]['median_seconds']['after']:.4f}",f"{bench[k]['speedup']:.2f}x"] for k,label in tech_names]
q_labels=[("01_zone_monthly","Zone product"),("02_weather_distance","Cached trips"),("03_air_quality_demand","Air product"),
          ("04_weather_variation","Weather product"),("05_peak_hours","Cached city hours"),("06_monthly_trend","Daily product")]
query_rows=[["Query / optimization","Before (s)","After (s)","Speedup"]]+[
    [f"Q{i+1}: {label}",f"{bench['query_'+q]['median_seconds']['before']:.4f}",f"{bench['query_'+q]['median_seconds']['after']:.4f}",f"{bench['query_'+q]['speedup']:.2f}x"] for i,(q,label) in enumerate(q_labels)]
product_rows=[["Product","Rows","KiB","Build (s)"]]+[
    [p['product_name'].replace('_',' '),f"{p['row_count']:,}",f"{p['storage_bytes']/1024:.2f}",f"{p['build_seconds']:.3f}"] for p in products]
cache_setup=statistics.median(x['setup_seconds'] for x in bench['query_02_weather_distance']['records']['after'])
cache_saving=bench['query_02_weather_distance']['median_seconds']['before']-bench['query_02_weather_distance']['median_seconds']['after']
benchpages=[
[("title","Query Performance Evaluation"),
 ("p","Lab 2 Benchmark Report | Actual measurements: 15 September 2026"),
 ("h","1. Environment and methodology"),
 ("p",f"Single {env['cpu']} machine, {int(env['logical_cpus'])} logical CPUs, {int(env['memory_bytes'])/1024**3:.0f} GiB RAM, local disk; {env['os']}. Python {env['python']}, Java {env['java']}, Spark {env['spark']}, Delta {env['delta_spark']}; local[4], requested 4 GB driver, eight SQL shuffle partitions and UTC Spark session. Local date interpretation is America/New_York. Exact storage device model was not collected."),
 ("p",f"Gold version 0 has 9,417,864 rows; the Jan-Mar local interval contains {valid['scope']['rows']:,}. Baseline AQE and automatic broadcast are disabled to isolate changes. Each variant is warmed once immediately before each of three measured collect() actions; pair order alternates. Timings exclude SQL DataFrame construction, explicit cache setup and product creation, but include execution and result collection. Spark caches are cleared between variants; OS caches are not flushed."),
 ("p","All ten comparison pairs passed result and schema checks, using exact integer/null comparisons and 1e-9 relative / 1e-8 absolute floating tolerance. Outputs and plans are saved. These are repeated local measurements, not confidence intervals or evidence about a distributed cluster."),
 ("h","2. Effects of the four required techniques"),
 ("table",tech_rows,[221,92,92,94]),
 ("p","Broadcast has the largest improvement among these four experiments (3.18x). The small zone lookup replaces a shuffled sort-merge join with BroadcastHashJoin BuildRight. Pruning shows explicit year/month PartitionFilters; both queries cover identical February local-time records. The optimized predicate includes UTC months 2 and 3 to avoid dropping the final local evening."),
 ("p","Cache reads an InMemoryRelation instead of repeatedly scanning the Delta source, but its median population/setup time is 9.214 s in the technique experiment. AQE has only a modest 1.04x median change: the final plan shows AQEShuffleRead coalesced, with little room for improvement at this small initial partition count. Do not extrapolate this small difference to larger workloads."),
 ("small","All table values are medians of three timed runs. Each experiment's raw timings and before/after SQL, EXPLAIN FORMATTED and post-execution plan are under evidence/benchmarks/ in the submission package.")],
[("h","3. Final original/optimized comparison for every query"),
 ("table",query_rows,[221,92,92,94]),
 ("p","Q1, Q3, Q4 and Q6 replace repeated full-data aggregation with equivalent queries on version-matched products. Q2 uses an explicit analysis cache; Q5 caches the city-hour intermediate. Their physical plans show small product scans or in-memory scans. Equality is checked for all output rows, not only counts or selected examples."),
 ("h","4. Materialization storage and creation cost"),
 ("table",product_rows,[245,80,80,94]),
 ("p",f"Products total {total_product_bytes:,} active Parquet bytes ({total_product_bytes/1024**2:.3f} MiB), across four active data files. This is {100*total_product_bytes/valid['gold_storage']['active_bytes']:.4f}% of Gold's {valid['gold_storage']['active_bytes']/1024**2:.2f} MiB active data. Delta transaction logs and the metadata registry are excluded from both data-size figures. Product table writes took {total_build:.3f} s in total; validation and registry writes are outside this build timer."),
 ("p",f"Q2's median cache setup is {cache_setup:.3f} s, compared with a {cache_saving:.3f} s steady-state saving. Under unchanged conditions, simple setup/saving amortization needs about {math.ceil(cache_setup/cache_saving)} repeated queries before setup is recovered. This estimate ignores eviction and competing workloads. Q5's smaller city-hour cache takes {statistics.median(x['setup_seconds'] for x in bench['query_05_peak_hours']['records']['after']):.3f} s to set up. A fast cached query is therefore not automatically a faster one-off workflow."),
 ("p","The products are regenerated from a pinned Gold snapshot. Registry metadata includes source version, configuration and schema, enabling stale products to be rejected before benchmarking. Source totals and product totals agree within each declared scope; the air product excludes trips lacking a borough instead of assigning a fabricated geography.")],
[("h","5. Variability, interpretation and limits"),
 ("table",[["Experiment","Before range (s)","After range (s)"]]+[
     [label,f"{min(v['execution_seconds'] for v in bench[k]['records']['before']):.4f} - {max(v['execution_seconds'] for v in bench[k]['records']['before']):.4f}",
            f"{min(v['execution_seconds'] for v in bench[k]['records']['after']):.4f} - {max(v['execution_seconds'] for v in bench[k]['records']['after']):.4f}"] for k,label in tech_names],[221,139,139]),
 ("p","Only three trials per variant were collected. The first cache baseline is slower than subsequent baseline trials, despite warm-up, illustrating residual JVM, scheduling and filesystem-cache effects. Median reporting and alternating order reduce sensitivity but do not eliminate it. Timings for the same Q2 SQL in its isolated technique experiment and later final comparison therefore differ; they are separate measurements, not interchangeable estimates."),
 ("p","Q4 is the most expensive original analytical query (1.712 s median): it builds zero-trip zone-hour combinations and groups demand by weather before ranking variation. Its weather product gives the largest final-query ratio (18.96x), but that shifts work to refresh time rather than removing it. Q3 and Q6 also repeatedly aggregate the large fact table; reusable hourly/daily summaries avoid that scan and shuffle for repeated access."),
 ("p","Data shape explains the results: 9.4 million trips join a 265-row lookup, so broadcast removes disproportionate shuffle work; monthly storage allows a bounded local-month query to omit irrelevant UTC partitions; small pre-aggregated outputs are cheap to read. AQE has limited impact with eight initial shuffle partitions. Caching adds memory pressure and significant preparation work; materialization adds freshness and maintenance obligations."),
 ("h","Correctness and source limitations"),
 ("p","The clean Gold table retains one unique ID per trip and no negative durations. All product totals were checked. Nevertheless, roughly 90% of trips lack air-quality context, and the UTC weather interpretation remains an unverified source assumption. Optimized equality proves that an optimization preserved the declared analysis, not that environmental provenance or causal interpretation is established. The source files were supplied without a weather timezone; no sensitivity experiment was performed."),
 ("h","Recommendations for ten cities"),
 ("p","Introduce city-aware keys and timezone configuration, profile skew and table sizes before selecting broadcast/partition strategies, compact small files and bound cache use. Publish source-version-aware product refreshes and monitor freshness. Test on an appropriate cluster with repeated workloads and representative concurrency before using these local ratios for capacity planning."),
 ("small","Evidence inventory: benchmark_all.json contains all 60 timed executions; products.json records row/file/size/build metrics; benchmark_environment.json records hardware/software; final_validation.json records scope and product totals; tests.xml records the automated test outcome. README gives commands to reproduce every step.")],
]

render("Lab2_Design_Report","Design Report",design)
render("Lab2_Benchmark_Report","Benchmark Report",benchpages)
print("Generated two reports from recorded full-data evidence.")
