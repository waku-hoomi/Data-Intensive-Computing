import sys
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from retail.common import spark_session


@pytest.fixture(scope="session")
def spark():
    session = spark_session("retail-correctness-tests", 2)
    yield session
    session.stop()
