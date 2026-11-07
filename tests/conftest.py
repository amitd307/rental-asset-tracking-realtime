import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# PySpark converts timestamps to the driver's local zone on collect(); pin it.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="session")
def root():
    return ROOT
