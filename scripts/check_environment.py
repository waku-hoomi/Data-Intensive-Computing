"""Verify the installed runtime with a real Delta round-trip."""
import importlib.metadata
import hashlib
import json
import platform
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from urban_platform.utils.spark_session import get_spark


def sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

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
            "hadoop": spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
            "master": spark.sparkContext.master,
            "roundtrip_rows": 10,
            "winutils_sha256": sha256(ROOT / "hadoop/bin/winutils.exe"),
            "hadoop_dll_sha256": sha256(ROOT / "hadoop/bin/hadoop.dll"),
        }
        target = ROOT / "artifacts" / "environment.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(record, indent=2))
        print(json.dumps(record, indent=2))
    finally:
        spark.stop()
