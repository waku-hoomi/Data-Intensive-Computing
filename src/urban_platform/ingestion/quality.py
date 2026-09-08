"""Generic data-quality checks applied uniformly to every dataset.

These are the checks Task 3 asks for: duplicate records, missing primary
keys, invalid timestamps, invalid numerical values. Kept dataset-agnostic
by working purely off the DatasetConfig (primary_key, timestamp_cols,
numeric_rules) instead of hardcoded column names, so a new dataset gets
these checks for free just by declaring its config in datasets.yaml.
"""
from dataclasses import dataclass, field
from typing import Dict

from pyspark.sql import DataFrame, Window, functions as F

from urban_platform.utils.config import DatasetConfig


@dataclass
class QualityReport:
    input_rows: int
    output_rows: int
    rejected_rows: int
    rejected_by_reason: Dict[str, int] = field(default_factory=dict)


def apply_quality_checks(df: DataFrame, cfg: DatasetConfig) -> "tuple[DataFrame, QualityReport]":
    input_rows = df.count()

    reject_reason = F.lit(None).cast("string")

    # 1) Missing primary key -> any PK column is NULL
    if cfg.primary_key:
        pk_null = F.lit(False)
        for col in cfg.primary_key:
            pk_null = pk_null | F.col(col).isNull()
        reject_reason = F.when(pk_null, F.lit("missing_primary_key")).otherwise(reject_reason)

    # 2) Invalid timestamps -> NULL after parsing, or outside a sane range
    for ts_col in cfg.timestamp_cols:
        invalid_ts = F.col(ts_col).isNull() | (F.col(ts_col) < F.lit("2000-01-01")) | (
            F.col(ts_col) > F.lit("2100-01-01")
        )
        reject_reason = F.when(invalid_ts, F.lit(f"invalid_timestamp:{ts_col}")).otherwise(reject_reason)

    # 3) Invalid numerical values -> outside declared [min, max] (nulls are
    #    allowed through -- a missing measurement is not the same defect as
    #    an impossible one, see Task 4 "rules for handling missing values")
    for rule in cfg.numeric_rules:
        col = F.col(rule.column)
        conditions = []
        if rule.min is not None:
            conditions.append(col < F.lit(rule.min))
        if rule.max is not None:
            conditions.append(col > F.lit(rule.max))
        if conditions:
            out_of_range = col.isNotNull()
            combined = conditions[0]
            for c in conditions[1:]:
                combined = combined | c
            out_of_range = out_of_range & combined
            reject_reason = F.when(out_of_range, F.lit(f"invalid_value:{rule.column}")).otherwise(reject_reason)

    df_flagged = df.withColumn("_reject_reason", reject_reason)

    # 4) Duplicate records -> exact duplicate primary key (kept as a
    #    separate flag since it requires a window, not a row-local check).
    #    The first occurrence (by insertion order) is kept; the rest are
    #    rejected -- consistent with "reject", never silently pick one.
    if cfg.primary_key:
        pk_window = Window.partitionBy(*cfg.primary_key).orderBy(F.lit(1))
        df_flagged = df_flagged.withColumn("_row_num_in_pk", F.row_number().over(pk_window))
        df_flagged = df_flagged.withColumn(
            "_reject_reason",
            F.when(
                (F.col("_row_num_in_pk") > 1) & F.col("_reject_reason").isNull(),
                F.lit("duplicate_primary_key"),
            ).otherwise(F.col("_reject_reason")),
        ).drop("_row_num_in_pk")

    df_flagged = df_flagged.cache()

    rejected_counts = (
        df_flagged.filter(F.col("_reject_reason").isNotNull())
        .groupBy("_reject_reason")
        .count()
        .collect()
    )
    rejected_by_reason = {row["_reject_reason"]: row["count"] for row in rejected_counts}
    total_rejected = sum(rejected_by_reason.values())

    clean_df = df_flagged.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    output_rows = input_rows - total_rejected

    report = QualityReport(
        input_rows=input_rows,
        output_rows=output_rows,
        rejected_rows=total_rejected,
        rejected_by_reason=rejected_by_reason,
    )
    return clean_df, report