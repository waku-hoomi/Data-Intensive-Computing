import datetime as dt
import pytest
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
from retail.common import load_config
from retail.clean import clean_transactions, build_baskets
from retail.ingest import identifier
from retail.mining import period_metrics


def rules(spark):
    return spark.createDataFrame([
        ("A", "B", "Product A", "Product B", 2/3, 10/9),
        ("A", "Z", "Product A", "Absent Z", 0.5, 2.0),
        ("Z", "A", "Absent Z", "Product A", 0.5, 2.0)],
        "antecedent string, consequent string, antecedent_name string, consequent_name string, confidence double, lift double")


def test_hand_calculated_metrics_keep_singletons_and_absent_rules(spark):
    baskets = spark.createDataFrame([("1", ["A", "B"]), ("2", ["A", "B"]),
              ("3", ["A"]), ("4", ["B"]), ("5", ["C"])], "invoice string, items array<string>")
    config = load_config()
    config.update(evaluation_min_antecedent_count=1, evaluation_min_consequent_count=1)
    values = {(r.antecedent, r.consequent): r for r in period_metrics(baskets, rules(spark), config, "test").collect()}
    ab = values["A", "B"]
    assert (ab.baskets, ab.antecedent_count, ab.consequent_count, ab.pair_count) == (5, 3, 3, 2)
    assert ab.support == pytest.approx(0.4)
    assert ab.confidence == pytest.approx(2/3)
    assert ab.lift == pytest.approx(10/9)
    assert ab.status == "retained"
    assert values["A", "Z"].confidence == 0
    assert values["A", "Z"].lift is None
    assert values["Z", "A"].confidence is None
    assert values["Z", "A"].status == "undefined"


def test_zero_cooccurrence_is_weakened_not_missing(spark):
    baskets = spark.createDataFrame([("1", ["A"]), ("2", ["B"])], "invoice string, items array<string>")
    config = load_config()
    config.update(evaluation_min_antecedent_count=1, evaluation_min_consequent_count=1)
    row = period_metrics(baskets, rules(spark).filter(F.col("consequent") == "B"), config, "test").first()
    assert row.confidence == row.lift == 0
    assert row.status == "weakened"


def test_small_sample_is_insufficient(spark):
    baskets = spark.createDataFrame([("1", ["A", "B"])], "invoice string, items array<string>")
    row = period_metrics(baskets, rules(spark).filter(F.col("consequent") == "B"), load_config(), "test").first()
    assert row.status == "insufficient"


def test_cleaning_conflicts_duplicates_and_time_boundaries(spark):
    date = dt.datetime(2011, 5, 31, 23, 59)
    def line(inv, stock, quantity=1., price=2., customer=None, country="UK", timestamp=date):
        return (inv, stock, "Description", quantity, timestamp, price, customer, country)
    raw = spark.createDataFrame([
        line("1", "A"), line("1", "A"), line("1", "A", quantity=2.), line("1", "PADS"),
        line("2", "SKU_B", timestamp=dt.datetime(2011, 6, 1)), line("C3", "A"),
        line("4", "A", quantity=0.), line("5", "POST"), line("6", "A", price=0.),
        line("7", "A", timestamp=dt.datetime(2011, 9, 1)),
        line("8", "A", country="UK"), line("8", "SKU_B", country="France"),
        line("9", "A", timestamp=dt.datetime(2010, 11, 30))],
        "invoice string, stock_code string, description string, quantity double, invoice_date timestamp, price double, customer_id string, country string")
    clean, stats, conflicts = clean_transactions(raw, load_config())
    baskets = {r.invoice: r for r in build_baskets(clean, load_config()).collect()}
    assert set(baskets) == {"1", "2"}
    assert baskets["1"].items == ["A", "PADS"]
    assert baskets["1"].period == "discovery"
    assert baskets["2"].items == ["SKU_B"] and baskets["2"].period == "evaluation"
    assert stats["clean_rows_missing_customer"] == 4
    assert stats["conflicting_invoices"] == 1
    assert stats["study_window_rows"] - sum(s["removed_rows"] for s in stats["stages"]) == stats["clean_rows"]
    clean.unpersist()


def test_fp_growth_frequencies_and_directions(spark):
    baskets = spark.createDataFrame([(["A", "B"],), (["A", "B"],), (["A"],), (["B"],), (["C"],)], "items array<string>")
    model = FPGrowth(minSupport=0.2, minConfidence=0.5).fit(baskets)
    freq = {frozenset(r.items): r.freq for r in model.freqItemsets.collect()}
    assert freq[frozenset(["A", "B"])] == 2
    found = {(tuple(r.antecedent), tuple(r.consequent)): r for r in model.associationRules.collect()}
    assert found[(('A',), ('B',))].confidence == pytest.approx(2/3)
    assert found[(('B',), ('A',))].support == pytest.approx(0.4)


def test_identifier_normalization_preserves_letter_products():
    assert identifier(12345.0) == "12345"
    assert identifier("PADS") == "PADS"
    assert identifier(None) is None
