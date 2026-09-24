"""Build an evidence-based report for the revised matched experiments."""
import json
import statistics
from pathlib import Path


def _storage_parts(snapshot):
    groups = snapshot['groups']
    business = [groups.get(name, {}) for name in ('bronze', 'gold', 'products')]
    parts = {
        'Active business data': snapshot['active_business_data_bytes'],
        'Retained business data files': sum(g.get('retained_data_bytes',0) for g in business),
        'Business CDF files': sum(g.get('cdf_bytes',0) for g in business),
        'Business Delta logs and other files': sum(g.get('delta_log_bytes',0)+g.get('other_bytes',0) for g in business),
        'Quarantine': groups.get('metadata/quarantine',{}).get('total_bytes',0),
        'Staging and change keys': sum(groups.get('metadata/'+n,{}).get('total_bytes',0) for n in ('classified','change_keys')),
        'Monitoring, publication and registry': sum(groups.get(n,{}).get('total_bytes',0) for n in ('monitoring','lab2_metadata')),
    }
    parts['Schema manifests and other metadata'] = snapshot['total_logical_bytes'] - sum(parts.values())
    return parts


def build_evaluation_markdown(root: Path) -> str:
    artifacts = root / 'artifacts/week3'
    read = lambda path: json.loads(path.read_text(encoding='utf-8'))
    evaluation = read(artifacts / 'evaluation.json')
    if evaluation.get('benchmark_version') != 2:
        raise ValueError('Run the revised Week 3 experiments before building the report')
    trials = evaluation['trials']
    delayed_host = any(o.get('kind') == 'host_response_delay'
                       for o in evaluation.get('observations', []))
    host_note = (
        "The first trial coincided with a prolonged delay in the host terminal's "
        "response during weather refresh. We retain the observation rather than "
        "remove it after seeing the result. Together with the small sample, this "
        "limits attribution of timing differences to the refresh strategy alone."
        if delayed_host else "")
    monitor_delay_note = (
        "The first monitoring difference also includes the host delay noted in "
        "Section 3, so it cannot be attributed entirely to event logging. "
        if delayed_host else "")
    records = [(p.stat().st_mtime_ns, read(p)) for p in artifacts.glob('*.json')]
    baseline = max((p for p in records if p[1].get('kind') == 'baseline'
                    and p[1].get('status') == 'success'), key=lambda p:p[0])[1]['bronze']
    median = lambda key: statistics.median(t[key] for t in trials)
    labels = {'taxi_zone_lookup':'Taxi zones', 'weather':'Weather',
              'air_quality':'Air quality', 'taxi_trips':'Taxi trips'}
    baseline_table = '\n'.join(
        f"| {label} | {baseline[n]['processed']:,} | {baseline[n]['inserted']:,} | "
        f"{baseline[n]['duplicates']:,} | {baseline[n]['rejected']:,} |"
        for n,label in labels.items())
    update_table = '\n'.join(
        f"| {label} | {trials[0]['bronze_metrics'][n]['processed']:,} | "
        f"{trials[0]['bronze_metrics'][n]['inserted']:,} | {trials[0]['bronze_metrics'][n]['updated']:,} | "
        f"{trials[0]['bronze_metrics'][n]['duplicates']:,} | {trials[0]['bronze_metrics'][n]['rejected']:,} |"
        for n,label in labels.items())
    times = '\n'.join(f"| {t['trial']} | {t['incremental_total_seconds']:.1f} | {t['full_total_seconds']:.1f} | "
        f"{t['incremental_gold_seconds']:.1f} | {t['full_gold_seconds']:.1f} |" for t in trials)
    product_table = '\n'.join(
        f"| {label} | {trials[0]['incremental_products'][n]['mode']} | "
        + ' | '.join(f"{t['incremental_products'][n]['seconds']:.1f}" for t in trials) + ' |'
        for n,label in [('daily_mobility_summary','Daily mobility'),('taxi_zone_statistics','Taxi-zone statistics'),
                        ('weather_impact_summary','Weather impact'),('air_quality_impact_summary','Air-quality impact')])
    product_header = '| Product | Refresh mode | ' + ' | '.join(f"Trial {t['trial']} (s)" for t in trials) + ' |'
    product_separator = '| --- | --- | ' + ' | '.join('---:' for _ in trials) + ' |'
    overhead_table = '\n'.join(
        f"| {t['trial']} | {t['validation_pair']['enabled_seconds']:.2f} | {t['validation_pair']['disabled_seconds']:.2f} | "
        f"{t['validation_overhead_seconds']:.2f} | {t['monitoring_off_total_seconds']:.1f} | {t['monitoring_overhead_seconds']:.1f} |"
        for t in trials)
    last = trials[-1]
    monitoring_interpretation = (
        f"The negative difference in trial {last['trial']} does not mean logging improves "
        "performance; it shows that paired wall times do not give a precise estimate "
        "of this cost on the local host."
        if last['monitoring_overhead_seconds'] < 0 else
        "More repetitions on a stable host are needed for a precise estimate of this cost.")
    before,after = _storage_parts(last['before_storage']),_storage_parts(last['after_storage'])
    storage_table = '\n'.join(f'| {name} | {after[name]-before[name]:,} |' for name in before)
    frequency = read(artifacts/'monitoring/01_validation_frequency.json')
    longest = read(artifacts/'monitoring/02_longest_processing.json')[0]
    fail_counts = ', '.join(f"{labels.get(r['dataset_name'],r['dataset_name'])}: {r['failed_validation_executions']}/{r['executions']}" for r in frequency)
    return f"""# Week 3 Evaluation Report

ID2221 Data-Intensive Computing | September 2026

## 1. Experimental method

We evaluated update correctness, refresh cost and the overhead of validation and
monitoring on the course datasets. The local environment used Python 3.12.14,
Java 17.0.20.1, PySpark 4.0.4 and Delta Lake 4.0.1 on Windows, with local[4]
and temporary storage on D:. Each of {len(trials)} trials started three workspaces
from the same validated baseline and applied the same four update files.

The three variants were incremental refresh, full Gold/product rebuild, and
incremental refresh with event monitoring disabled. They share the same ingestion,
orchestration and publication code. In the first trial their order was incremental,
full, monitoring-off; the second reversed it. Spark caches were cleared between
variants, but operating-system caches were not. This controls the work performed
and reduces order bias without eliminating local timing noise. The monitoring
comparison also checks that disabling logging leaves analytical results unchanged.

## 2. Validation and update outcomes

The baseline revalidates the historical inputs using the Week 3 rules. Duplicates
and other rejected records are listed separately below.

| Dataset | Input rows | Accepted | Duplicates | Other rejects |
| --- | ---: | ---: | ---: | ---: |
{baseline_table}

Invalid historical rows are quarantined and excluded from analysis. A row may
violate multiple rules, so failure counts can exceed rejected-row counts. The
controlled release gave the following outcomes in the first trial; both trials
use the same update files and record outcomes.

| Dataset | Processed | Inserted | Updated | Duplicates | Rejected |
| --- | ---: | ---: | ---: | ---: | ---: |
{update_table}

Taxi updates add 7.5% new trips and 1.5% exact copies of the original rows.
Weather adds humidity, air quality adds AQI, and historical corrections exercise
propagation to existing trips. Invalid examples remain in separate tests.

<!-- PAGEBREAK -->

## 3. Update and analytical refresh performance

| Trial | Incremental total (s) | Full total (s) | Incremental Gold (s) | Full Gold (s) |
| --- | ---: | ---: | ---: | ---: |
{times}

In trial {last['trial']}, incremental processing took {last['incremental_total_seconds']:.1f} s,
compared with {last['full_total_seconds']:.1f} s for the full workflow. The incremental path touched
{trials[0]['incremental_affected_trips']:,} trips: zone-label corrections can spread
to a large part of Gold. These results support measuring correction size before
choosing MERGE or a full rebuild; we have not established a switching threshold.

{product_header}
{product_separator}
{product_table}

Group refresh replaces affected aggregation groups. Weather is rebuilt when the
analysis window advances because its observed-hour denominators change. Full
product rebuilding took a median of {median('full_product_seconds'):.1f} s.
Product timings include registry updates in both variants.

{host_note}

## 4. Validation and monitoring overhead

For validation, we fully materialised the same transformed input and reference
rows before timing either arm. Both arms consume every column through the same
row-hash aggregation. The enabled arm checks schema and values, then recombines
accepted and rejected rows so both sinks have the same input multiset. Preparation
is measured separately and excluded. This estimates validation computation over
prepared inputs; it excludes transformation, MERGE and quarantine writes.

| Trial | Validation on (s) | Validation off (s) | Difference (s) | Monitoring-off total (s) | Monitoring difference (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
{overhead_table}

The median additional validation time was {median('validation_overhead_seconds'):.2f} s.
The observed monitoring difference is the incremental total in Section 3 minus
the otherwise identical monitoring-off run above. Direct event writes took
{median('monitoring_write_seconds'):.1f} s at the median. Publication and product
registry writes remain enabled in both arms. All raw differences, including any
negative values, are retained; two trials are insufficient to separate small
overheads from scheduling, disk and cache variation with confidence.
{monitor_delay_note}{monitoring_interpretation}

<!-- PAGEBREAK -->

## 5. Storage and monitoring results

We counted every file under the trial data directory and reconciled the categories
with its total. The table shows trial {last['trial']}'s change from the same baseline.
No VACUUM ran: obsolete data, CDF, logs, quarantine and staging are retained.
Sizes are logical file bytes, with hard links counted per path, not additional
disk allocation. Input files and evaluation clones are outside this platform total.

| Component | Change (bytes) |
| --- | ---: |
{storage_table}
| Total | {last['storage_delta_bytes']['total_logical_bytes']:,} |

Active business data grew by {last['storage_delta_bytes']['active_business_data_bytes']:,}
bytes. Operational and retained files grew by
{last['storage_delta_bytes']['operational_and_retained_bytes']:,} bytes. Separating
these quantities avoids treating all new data as infrastructure overhead. Storage
cost depends on retention; the inventory captures this experiment before cleanup.

The saved production monitoring snapshot reports validation events in these
execution counts: {fail_counts}. {labels[longest['dataset_name']]} had the longest
Bronze duration, {longest['longest_seconds']:.1f} s. These values mix baseline loads
and retries. Duplicate replays count as validation events even with zero invalid
rows, so we interpret them alongside rejection reasons, stage duration and status.
Source freshness, shuffle spill and quarantine age would help diagnose a deployed
system, but are not measured here.

## 6. Correctness and maintainability

Both refresh variants and both monitoring settings produced matching Gold hash
summaries, four product results and all six query results. Final Gold contains
{trials[0]['verification']['gold']['rows']:,} rows. Hash summaries are strong evidence
of agreement rather than collision-free row-by-row proof; numeric product results
are compared with floating-point tolerance.

Recovery tests use real Delta tables and inject failures after Gold and after a
product commit. They check that the old publication remains visible, retries
match complete product recomputation, pending keys survive a new batch, and a
completed replay performs no analytical refresh. A multi-commit reversal test
checks that intermediate product groups remain eligible for repair.

Week 1's common schema and configuration-driven loader reduced changes to
ingestion and validation. Cross-table publication and correction propagation
required the most work. New datasets can reuse existing rule types but still
need keys, references and sometimes transforms or integration code. We would
next add a rule registry and a measured choice between partial and full refresh.
Multiple writers would require coordination; recovery also depends on retained
Delta history. This local experiment does not establish cluster scalability.
More cities would require city-aware keys, partition pruning, skew tests and
file compaction. The weather timezone remains an unverified UTC assumption.
"""
