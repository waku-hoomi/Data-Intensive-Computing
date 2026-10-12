import hashlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_config(path=None):
    return json.loads(Path(path or ROOT / "config/project.json").read_text())


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def spark_session(name, partitions=8):
    from pyspark.sql import SparkSession
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    # Preserve dataset calendar timestamps; Python and Spark agree on naive datetime serialization.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    local_java = ROOT / ".runtime/java_home"
    if "JAVA_HOME" not in os.environ and local_java.exists():
        os.environ["JAVA_HOME"] = str(local_java)
    temp = ROOT / ".runtime/spark-tmp"
    temp.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master(os.environ.get("RETAIL_SPARK_MASTER", "local[4]"))
             .appName(name).config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", str(partitions))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .config("spark.local.dir", str(temp)).getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
