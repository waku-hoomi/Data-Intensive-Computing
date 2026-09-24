"""Matched validation controls and complete on-disk storage accounting."""
from __future__ import annotations

import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from urban_platform.ingestion.pipeline import (
    _load_raw, _references, _read_raw_schema, _validate_raw_schema,
)
from urban_platform.ingestion.quality import validate_records


def fingerprint(df) -> dict:
    """Force evaluation of every value, including columns count() can prune."""
    hashed = df.select(F.xxhash64(*[F.col(c) for c in sorted(df.columns)]).alias('h'))
    return hashed.agg(F.count('*').alias('rows'),
                      F.sum(F.col('h').cast('decimal(38,0)')).alias('hash_sum'),
                      F.min('h').alias('hash_min'), F.max('h').alias('hash_max')).first().asDict()


def validation_pair(spark, cfg, updates: Path, files: dict, enabled_first: bool) -> dict:
    """Measure row/schema checks over identical fully materialised inputs.

    Both arms consume all original columns and all input rows. The enabled arm
    recombines accepted/rejected rows so output cardinality cannot bias the sink.
    Input transformation/cache setup is reported separately and excluded from
    the paired difference. No production validation switch is introduced.
    """
    result = {}
    for name, filename in files.items():
        ds = cfg.get_dataset(name)
        raw = _load_raw(spark, ds, cfg.raw_data_dir, str(updates / filename))
        prepared_at = time.perf_counter()
        data = ds.resolve_transform_fn()(raw).persist()
        references = {n: f.persist() for n, f in _references(spark, cfg, ds).items()}
        try:
            expected = fingerprint(data)
            for frame in references.values():
                fingerprint(frame)
            item = {'preparation_seconds': time.perf_counter() - prepared_at,
                    'rows': expected['rows'], 'order': ['enabled', 'disabled'] if enabled_first else ['disabled', 'enabled']}
            for enabled in ([True, False] if enabled_first else [False, True]):
                start = time.perf_counter()
                if enabled:
                    _validate_raw_schema(raw, ds, strict=True, baseline_schema=_read_raw_schema(cfg, name))
                    clean, rejected, report = validate_records(data, ds, references)
                    actual = fingerprint(clean.unionByName(rejected.select(*data.columns)))
                    item['invalid_or_duplicate_rows'] = report.rejected_rows
                else:
                    actual = fingerprint(data)
                item['enabled_seconds' if enabled else 'disabled_seconds'] = time.perf_counter() - start
                if actual != expected:
                    raise AssertionError(f'{name}: validation control changed row multiset')
            item['additional_seconds'] = item['enabled_seconds'] - item['disabled_seconds']
            item['identical_input_and_sink'] = True
            result[name] = item
        finally:
            data.unpersist(blocking=True)
            for frame in references.values():
                frame.unpersist(blocking=True)
    return {'datasets': result,
            'enabled_seconds': sum(x['enabled_seconds'] for x in result.values()),
            'disabled_seconds': sum(x['disabled_seconds'] for x in result.values()),
            'additional_seconds': sum(x['additional_seconds'] for x in result.values())}


def storage_inventory(spark, root: Path) -> dict:
    """Account for every file below data/, without claiming disk-allocation bytes.

    Hard links are counted by path (logical file bytes). Active bytes come from
    Delta metadata; retained data bytes are all ordinary Parquet minus active.
    No VACUUM is run, so obsolete files and CDF remain in the inventory.
    """
    data = root / 'data'
    groups = {}
    for path in data.rglob('*'):
        if not path.is_file():
            continue
        rel = path.relative_to(data)
        if rel.parts[0] in {'bronze', 'gold', 'products', 'monitoring', 'lab2_metadata'}:
            group = rel.parts[0]
        elif rel.parts[0] == '_metadata' and len(rel.parts) > 1:
            group = 'metadata/' + rel.parts[1]
        else:
            group = 'other/' + rel.parts[0]
        counts = groups.setdefault(group, dict(data_parquet_bytes=0, cdf_bytes=0,
            delta_log_bytes=0, other_bytes=0, total_bytes=0, file_count=0))
        size = path.stat().st_size
        category = ('delta_log_bytes' if '_delta_log' in rel.parts else
                    'cdf_bytes' if '_change_data' in rel.parts else
                    'data_parquet_bytes' if path.suffix == '.parquet' else 'other_bytes')
        counts[category] += size
        counts['total_bytes'] += size
        counts['file_count'] += 1
    for group in ('bronze', 'gold', 'products', 'monitoring', 'lab2_metadata'):
        if group not in groups:
            continue
        active = 0
        for table in (data / group).iterdir():
            if table.is_dir() and (table / '_delta_log').is_dir():
                active += int(DeltaTable.forPath(spark, str(table)).detail().first().sizeInBytes)
        groups[group]['active_data_bytes'] = active
        retained = groups[group]['data_parquet_bytes'] - active
        if retained < 0:
            raise AssertionError(f'{group}: inventory omitted active files')
        groups[group]['retained_data_bytes'] = retained
    active_business = sum(groups.get(g, {}).get('active_data_bytes', 0)
                          for g in ('bronze', 'gold', 'products'))
    total = sum(g['total_bytes'] for g in groups.values())
    return {'groups': groups, 'total_logical_bytes': total,
            'active_business_data_bytes': active_business,
            'operational_and_retained_bytes': total - active_business,
            'policy': 'No VACUUM; all retained files and CDF counted. Logical file bytes; hard links counted per path.'}
