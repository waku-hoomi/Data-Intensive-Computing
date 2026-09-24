"""Generic data-quality checks applied uniformly to every dataset.

These are the checks Task 3 asks for: duplicate records, missing primary
keys, invalid timestamps, invalid numerical values. Kept dataset-agnostic
by working purely off the DatasetConfig (primary_key, timestamp_cols,
numeric_rules) instead of hardcoded column names, so a new dataset gets
these checks for free just by declaring its config in datasets.yaml.
"""
from dataclasses import dataclass, field
from typing import Dict, Optional

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
        if rule.column not in df.columns:
            continue  # A declared, additive Week 3 field may not exist in Week 1 data.
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

    if {"pickup_ts", "dropoff_ts"}.issubset(df.columns):
        reject_reason = F.when(F.col("dropoff_ts") < F.col("pickup_ts"),
                               F.lit("negative_trip_duration")).otherwise(reject_reason)
        df_flagged = df.withColumn("_reject_reason", reject_reason)

    # 4) Duplicate records -> exact duplicate primary key (kept as a
    #    separate flag since it requires a window, not a row-local check).
    #    Prefer valid rows, then a deterministic serialization order.
    #    Fully identical duplicates are indistinguishable by design.
    if cfg.primary_key:
        stable_row = F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)]),
                              {"ignoreNullFields": "false"})
        pk_window = Window.partitionBy(*cfg.primary_key).orderBy(
            F.col("_reject_reason").isNotNull().asc(), stable_row.asc())
        df_flagged = df_flagged.withColumn("_row_num_in_pk", F.row_number().over(pk_window))
        df_flagged = df_flagged.withColumn(
            "_reject_reason",
            F.when(
                (F.col("_row_num_in_pk") > 1) & F.col("_reject_reason").isNull(),
                F.lit("duplicate_primary_key"),
            ).otherwise(F.col("_reject_reason")),
        ).drop("_row_num_in_pk")

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


def validate_records(
    df: DataFrame,
    cfg: DatasetConfig,
    reference_tables: Optional[Dict[str, DataFrame]] = None,
) -> "tuple[DataFrame, DataFrame, QualityReport]":
    """Strict, row-level validation for Week 3, retaining every rejection reason.

    The quarantine frame has all standardized columns and `_reject_reason`.
    Foreign-key reference frames are supplied by the caller so this module
    remains independent of storage locations.
    """
    references = reference_tables or {}
    flagged = df
    reasons = []
    for column in [*cfg.required_fields, *[c for c in cfg.required_when_present if c in df.columns]]:
        if column not in df.columns:
            raise ValueError(f"{cfg.name}: standardized required field missing: {column}")
        reasons.append(F.when(F.col(column).isNull(), F.lit(f"missing_required:{column}")))
    for column in cfg.timestamp_cols:
        if column in df.columns:
            bad = F.col(column).isNull() | (F.col(column) < F.lit("2000-01-01")) | (F.col(column) >= F.lit("2100-01-01"))
            reasons.append(F.when(bad, F.lit(f"invalid_timestamp:{column}")))
    for rule in cfg.numeric_rules:
        if rule.column not in df.columns:
            continue
        value = F.col(rule.column)
        bad = F.isnan(value.cast("double"))
        if rule.min is not None:
            bad = bad | (value < F.lit(rule.min))
        if rule.max is not None:
            bad = bad | (value > F.lit(rule.max))
        reasons.append(F.when(bad, F.lit(f"invalid_value:{rule.column}")))
    if {"pickup_ts", "dropoff_ts"}.issubset(df.columns):
        reasons.append(F.when(F.col("dropoff_ts") < F.col("pickup_ts"), F.lit("negative_trip_duration")))

    for rule in cfg.foreign_keys:
        if rule.dataset not in references:
            raise ValueError(f"{cfg.name}: missing foreign-key reference {rule.dataset}")
        marker = f"_fk_exists_{rule.column}"
        ref = references[rule.dataset].select(F.col(rule.ref_column).alias(rule.column)).distinct().withColumn(marker, F.lit(1))
        flagged = flagged.join(F.broadcast(ref), on=rule.column, how="left")
        reasons.append(F.when(F.col(rule.column).isNotNull() & F.col(marker).isNull(),
                              F.lit(f"missing_foreign_key:{rule.column}")))

    if reasons:
        flagged = flagged.withColumn("_reject_reasons", F.array(*reasons))
        flagged = flagged.withColumn("_reject_reasons", F.expr("filter(_reject_reasons, x -> x IS NOT NULL)"))
    else:
        flagged = flagged.withColumn("_reject_reasons", F.array().cast("array<string>"))
    if cfg.primary_key:
        # A valid row wins over an invalid row with the same key. Among
        # conflicting valid rows, choose a deterministic payload ordering.
        payload = F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)]),
                            {"ignoreNullFields": "false"})
        window = Window.partitionBy(*cfg.primary_key).orderBy(
            F.size("_reject_reasons").asc(), payload.asc())
        flagged = flagged.withColumn("_batch_row_number", F.row_number().over(window))
        flagged = flagged.withColumn(
            "_reject_reasons",
            F.when((F.col("_batch_row_number") > 1) & (F.size("_reject_reasons") == 0),
                   F.array(F.lit("duplicate_primary_key"))).otherwise(F.col("_reject_reasons")),
        ).drop("_batch_row_number")
    flagged = flagged.withColumn("_reject_reason", F.concat_ws(";", F.col("_reject_reasons")))
    clean = flagged.filter(F.size("_reject_reasons") == 0).select(*df.columns)
    rejected = flagged.filter(F.size("_reject_reasons") > 0).select(*df.columns, "_reject_reason")
    # A single grouped pass computes both row totals and per-reason counts.
    # Counting clean/rejected separately would repeat the PK window shuffle.
    groups = flagged.groupBy("_reject_reason").count().collect()
    reason_counts = {}
    input_rows = rejected_rows = 0
    for row in groups:
        count = row["count"]
        input_rows += count
        if row["_reject_reason"]:
            rejected_rows += count
            for reason in row["_reject_reason"].split(";"):
                reason_counts[reason] = reason_counts.get(reason, 0) + count
    return clean, rejected, QualityReport(input_rows, input_rows - rejected_rows,
                                           rejected_rows, reason_counts)
