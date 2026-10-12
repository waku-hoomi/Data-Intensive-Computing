from pyspark.sql import functions as F

from .common import ROOT, save_json

KEY_FIELDS = ["invoice", "stock_code", "description", "quantity", "invoice_date", "price", "customer_id", "country"]


def clean_transactions(raw, config):
    """Sequential, disjoint quality accounting within the declared study window."""
    stats = {"raw_rows": raw.count()}
    window = raw.filter((F.col("invoice_date") >= F.lit(config["discovery_start"]).cast("timestamp")) &
                        (F.col("invoice_date") < F.lit(config["evaluation_end"]).cast("timestamp"))).cache()
    stats["study_window_rows"] = window.count()
    stages = []
    current = window

    def retain(name, condition):
        nonlocal current
        before = current.count()
        following = current.filter(F.coalesce(condition, F.lit(False))).cache()
        after = following.count()
        stages.append({"stage": name, "removed_rows": before - after, "remaining_rows": after})
        if current is not window:
            current.unpersist()
        current = following

    retain("cancellations", F.col("invoice").isNull() | ~F.upper(F.col("invoice")).startswith("C"))
    retain("invalid_sales", F.col("invoice").isNotNull() & (F.length("invoice") > 0) &
           F.col("stock_code").isNotNull() & (F.length("stock_code") > 0) &
           F.col("invoice_date").isNotNull() & (F.col("quantity") > 0) &
           (F.col("price") > 0) & ~F.isnan("quantity") & ~F.isnan("price") &
           (F.abs(F.col("quantity")) != float("inf")) & (F.abs(F.col("price")) != float("inf")))
    excluded = (current.filter(F.upper("stock_code").isin(config["excluded_codes"]))
                .groupBy("stock_code", "description").count().orderBy(F.desc("count"), "stock_code", "description"))
    stats["excluded_nonproduct_entries"] = [r.asDict() for r in excluded.collect()]
    retain("nonproduct_entries", ~F.upper("stock_code").isin(config["excluded_codes"]))
    before = current.count()
    dedup = current.select(*KEY_FIELDS).dropDuplicates(KEY_FIELDS).cache()
    after = dedup.count()
    stages.append({"stage": "exact_duplicate_lines", "removed_rows": before - after, "remaining_rows": after})
    current.unpersist()
    # Non-null customer IDs and countries must agree within an invoice. Missing IDs remain valid.
    conflicts = (dedup.groupBy("invoice").agg(F.countDistinct("invoice_date").alias("dates"),
                 F.countDistinct("customer_id").alias("customers"), F.countDistinct("country").alias("countries"))
                 .filter((F.col("dates") > 1) | (F.col("customers") > 1) | (F.col("countries") > 1)))
    stats["conflicting_invoices"] = conflicts.count()
    clean = dedup.join(conflicts.select("invoice"), "invoice", "left_anti").cache()
    remaining = clean.count()
    stages.append({"stage": "conflicting_invoices", "removed_rows": after - remaining, "remaining_rows": remaining})
    stats["stages"] = stages
    stats["clean_rows"] = remaining
    stats["clean_rows_missing_customer"] = clean.filter(F.col("customer_id").isNull()).count()
    stats["clean_rows_missing_description"] = clean.filter(F.col("description").isNull()).count()
    window.unpersist()
    dedup.unpersist()
    return clean, stats, conflicts


def build_baskets(clean, config):
    return (clean.groupBy("invoice").agg(F.min("invoice_date").alias("invoice_date"),
            F.sort_array(F.collect_set("stock_code")).alias("items"))
            .withColumn("basket_size", F.size("items"))
            .withColumn("month", F.date_format("invoice_date", "yyyy-MM"))
            .withColumn("period", F.when(F.col("invoice_date") < F.lit(config["discovery_end"]).cast("timestamp"),
                                        "discovery").otherwise("evaluation")))


def prepare(spark, config, storage):
    raw = spark.read.parquet(str(ROOT / "data/converted"))
    clean, stats, conflicts = clean_transactions(raw, config)
    clean.write.mode("overwrite").parquet(storage + "/clean_lines")
    conflicts.write.mode("overwrite").parquet(storage + "/conflicts")
    baskets = build_baskets(clean, config).cache()
    stats["baskets"] = baskets.count()
    stats["basket_summary"] = [r.asDict() for r in baskets.groupBy("period", "month").agg(
        F.count("*").alias("baskets"), F.sum(F.when(F.col("basket_size") == 1, 1).otherwise(0)).alias("singletons"),
        F.avg("basket_size").alias("mean_size"), F.max("basket_size").alias("max_size")).orderBy("month").collect()]
    stats["basket_size_quantiles"] = baskets.approxQuantile("basket_size", [0.5, 0.9, 0.99, 1.0], 0.001)
    stats["distinct_products"] = clean.select("stock_code").distinct().count()
    baskets.write.mode("overwrite").partitionBy("period", "month").parquet(storage + "/baskets")
    save_json(ROOT / "results/quality.json", stats)
    baskets.unpersist()
    clean.unpersist()
    return stats
