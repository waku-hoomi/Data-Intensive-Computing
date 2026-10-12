import csv
import time
from pathlib import Path

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F
from pyspark.sql import Window

from .common import ROOT, save_json, sha256


def export_csv(frame, path, order=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if order:
        frame = frame.orderBy(*order)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=frame.columns)
        writer.writeheader()
        for row in frame.toLocalIterator():
            writer.writerow(row.asDict())


def discover(spark, config, storage):
    baskets = spark.read.parquet(storage + "/baskets")
    discovery = baskets.filter(F.col("period") == "discovery").select("invoice", "items").cache()
    n = discovery.count()
    if n == 0:
        raise ValueError("No discovery baskets")
    item_counts = discovery.select(F.explode("items").alias("item")).groupBy("item").count()
    pilots = []
    final_rules = None
    for support in config["pilot_supports"]:
        start = time.perf_counter()
        model = FPGrowth(itemsCol="items", minSupport=support,
                         minConfidence=config["min_confidence"], numPartitions=config["shuffle_partitions"]).fit(discovery)
        count = model.freqItemsets.count()
        pair_rules = model.associationRules.filter((F.size("antecedent") == 1) & (F.size("consequent") == 1))
        rules = (pair_rules.select(F.col("antecedent")[0].alias("antecedent"),
                  F.col("consequent")[0].alias("consequent"), "support", "confidence", "lift")
                 .withColumn("pair_count", F.round(F.col("support") * n).cast("long"))
                 .join(item_counts.select(F.col("item").alias("antecedent"), F.col("count").alias("antecedent_count")), "antecedent")
                 .join(item_counts.select(F.col("item").alias("consequent"), F.col("count").alias("consequent_count")), "consequent")
                 .filter((F.col("lift") >= config["min_lift"]) & (F.col("pair_count") >= config["min_pair_count"]) &
                         (F.col("antecedent_count") >= config["min_antecedent_count"])))
        selected = rules.count()
        pilots.append({"min_support": support, "frequent_itemsets": count,
                       "selected_pair_rules": selected, "seconds": time.perf_counter() - start})
        print(f"Discovery support={support}: {count} itemsets, {selected} selected rules", flush=True)
        if support == config["min_support"]:
            final_rules = rules.cache()
            final_rules.count()
            model.write().overwrite().save(str(ROOT / "models/fpgrowth"))
            model.freqItemsets.write.mode("overwrite").parquet(storage + "/frequent_itemsets")
    if final_rules is None or not final_rules.count():
        raise ValueError("Configured discovery parameters produced no rules; adjust only using discovery data")
    # Human-readable names use discovery data alone, modal description with lexical tie-break.
    names = (spark.read.parquet(storage + "/clean_lines")
             .filter(F.col("invoice_date") < F.lit(config["discovery_end"]).cast("timestamp"))
             .filter(F.col("description").isNotNull()).groupBy("stock_code", "description").count()
             .withColumn("rank", F.row_number().over(Window.partitionBy("stock_code").orderBy(F.desc("count"), "description")))
             .filter(F.col("rank") == 1))
    final_rules = (final_rules.join(names.select(F.col("stock_code").alias("antecedent"), F.col("description").alias("antecedent_name")), "antecedent", "left")
                   .join(names.select(F.col("stock_code").alias("consequent"), F.col("description").alias("consequent_name")), "consequent", "left"))
    final_rules.write.mode("overwrite").parquet(storage + "/rules")
    export_csv(final_rules, ROOT / "results/discovery_rules.csv", ["antecedent", "consequent"])
    save_json(ROOT / "results/discovery.json", {"baskets": n, "pilots": pilots,
              "selected_rules": final_rules.count(), "config": config,
              "rules_csv_sha256": sha256(ROOT / "results/discovery_rules.csv"),
              "source_zip_sha256": __import__("json").loads((ROOT / "results/source_manifest.json").read_text())["zip_sha256"],
              "quality_sha256": sha256(ROOT / "results/quality.json")})
    discovery.unpersist()


def period_metrics(baskets, rules, config, label):
    """Every frozen directed rule survives a left join, including absent products."""
    n = baskets.count()
    if n == 0:
        raise ValueError(f"No baskets in period {label}")
    membership = baskets.select("invoice", "items", F.explode("items").alias("item"))
    singles = membership.groupBy("item").count()
    pairs = (membership.join(F.broadcast(rules.select("antecedent", "consequent")), F.col("item") == F.col("antecedent"))
             .filter(F.array_contains(F.col("items"), F.col("consequent")))
             .groupBy("antecedent", "consequent").count().withColumnRenamed("count", "pair_count"))
    metrics = (rules.select("antecedent", "consequent", "antecedent_name", "consequent_name",
               F.col("confidence").alias("discovery_confidence"), F.col("lift").alias("discovery_lift"))
               .join(singles.select(F.col("item").alias("antecedent"), F.col("count").alias("antecedent_count")), "antecedent", "left")
               .join(singles.select(F.col("item").alias("consequent"), F.col("count").alias("consequent_count")), "consequent", "left")
               .join(pairs, ["antecedent", "consequent"], "left")
               .fillna(0, subset=["antecedent_count", "consequent_count", "pair_count"])
               .withColumn("period", F.lit(label)).withColumn("baskets", F.lit(n))
               .withColumn("support", F.col("pair_count") / F.lit(n))
               .withColumn("confidence", F.when(F.col("antecedent_count") > 0, F.col("pair_count") / F.col("antecedent_count")))
               .withColumn("lift", F.when((F.col("antecedent_count") > 0) & (F.col("consequent_count") > 0),
                                           F.col("confidence") / (F.col("consequent_count") / F.lit(n))))
               .withColumn("confidence_retention", F.col("confidence") / F.col("discovery_confidence"))
               .withColumn("status", F.when((F.col("antecedent_count") == 0) | (F.col("consequent_count") == 0), "undefined")
                   .when((F.col("antecedent_count") < config["evaluation_min_antecedent_count"]) |
                         (F.col("consequent_count") < config["evaluation_min_consequent_count"]), "insufficient")
                   .when((F.col("lift") > 1) & (F.col("confidence_retention") >= config["confidence_retention"]), "retained")
                   .otherwise("weakened")))
    return metrics


def evaluate(spark, config, storage):
    import json
    frozen = json.loads((ROOT / "results/discovery.json").read_text())
    if frozen["config"] != config or frozen["rules_csv_sha256"] != sha256(ROOT / "results/discovery_rules.csv"):
        raise ValueError("Discovery config/rules changed; rediscover explicitly before evaluation")
    if frozen["quality_sha256"] != sha256(ROOT / "results/quality.json"):
        raise ValueError("Prepared baskets changed after discovery")
    rules = spark.read.parquet(storage + "/rules").cache()
    baskets = spark.read.parquet(storage + "/baskets").filter(F.col("period") == "evaluation").cache()
    windows = [("evaluation", baskets)] + [(m, baskets.filter(F.col("month") == m)) for m in ["2011-06", "2011-07", "2011-08"]]
    all_metrics = None
    for label, data in windows:
        frame = period_metrics(data, rules, config, label)
        all_metrics = frame if all_metrics is None else all_metrics.unionByName(frame)
    all_metrics = all_metrics.cache()
    all_metrics.write.mode("overwrite").partitionBy("period").parquet(storage + "/metrics")
    export_csv(all_metrics, ROOT / "results/temporal_metrics.csv", ["period", "antecedent", "consequent"])
    summary = [r.asDict() for r in all_metrics.groupBy("period", "status").count().orderBy("period", "status").collect()]
    monthly = all_metrics.filter(F.col("period") != "evaluation")
    all_retained = monthly.groupBy("antecedent", "consequent").agg(
        F.sum(F.when(F.col("status") == "retained", 1).otherwise(0)).alias("retained_months"))
    save_json(ROOT / "results/evaluation.json", {"summary": summary,
        "rules_retained_all_three_months": all_retained.filter(F.col("retained_months") == 3).count(),
        "rules": rules.count(), "evaluation_baskets": baskets.count(),
        "classification": "Descriptive: at least 30 antecedent and 30 consequent baskets; lift > 1 and confidence >= 80% of discovery.",
        "rules_csv_sha256": frozen["rules_csv_sha256"]})
    all_metrics.unpersist()
    rules.unpersist()
    baskets.unpersist()
