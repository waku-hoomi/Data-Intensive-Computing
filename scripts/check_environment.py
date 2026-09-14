"""Verify the installed runtime with a real Delta round-trip."""
import importlib.metadata
import json
import platform
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from urban_platform.utils.spark_session import get_spark

if __name__ == "__main__":
    spark = get_spark("lab2-environment-check")
    try:
        with TemporaryDirectory(prefix="lab2-smoke-", dir=ROOT / ".runtime") as temp:
            path = str(Path(temp) / "delta")
            spark.range(10).write.format("delta").save(path)
            assert spark.read.format("delta").load(path).count() == 10
        record = {
            "python": sys.version, "platform": platform.platform(),
            "spark": spark.version,
            "delta_spark": importlib.metadata.version("delta-spark"),
            "java": spark._jvm.java.lang.System.getProperty("java.version"),
            "master": spark.sparkContext.master,
            "roundtrip_rows": 10,
        }
        target = ROOT / "artifacts" / "environment.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(record, indent=2))
        print(json.dumps(record, indent=2))
    finally:
        spark.stop()
