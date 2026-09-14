import sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from urban_platform.utils.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("lab2-correctness-tests", shuffle_partitions=2)
    yield session
    session.stop()
