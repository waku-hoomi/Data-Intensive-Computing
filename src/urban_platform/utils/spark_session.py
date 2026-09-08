"""Spark/Delta session bootstrap shared by every entry point in the platform.

Kept in one place so ingestion, integration, and benchmarking scripts all
start Spark the same way (same Delta config, same local Hadoop shim on
Windows), instead of copy-pasting builder boilerplate everywhere.
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def _ensure_windows_hadoop_home() -> None:
    """On Windows, Spark/Hadoop needs winutils.exe + hadoop.dll to be findable.

    We ship a local copy under <project_root>/hadoop instead of relying on a
    machine-wide install, so the project is self-contained and reproducible
    on any Windows machine without admin rights.
    """
    if os.name != "nt":
        return
    hadoop_home = os.path.join(PROJECT_ROOT, "hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    if os.path.isdir(hadoop_bin):
        os.environ.setdefault("HADOOP_HOME", hadoop_home)
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def get_spark(app_name: str = "urban-data-platform", shuffle_partitions: int = 8) -> SparkSession:
    """Create (or fetch) a local SparkSession configured for Delta Lake.

    shuffle_partitions defaults low (8) because this project targets a
    single-machine, single-month-scale dataset (Task setup, see README) --
    the Spark default of 200 would create hundreds of tiny files on data
    this size, hurting both write and read performance.
    """
    _ensure_windows_hadoop_home()

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark