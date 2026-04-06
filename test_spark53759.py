"""
Pytest suite for SPARK-53759: createSimpleWorker broken on Windows with Python 3.12+.

Tests are ordered from JVM-only (always pass) to worker-dependent (crash on 3.12+).
Run with: pytest test_spark53759.py -v
"""
import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("SPARK-53759-repro")
        .config("spark.ui.enabled", "false")
        .config("spark.python.use.daemon", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# --- JVM-only / driver-side (always pass) ---

def test_spark_range(spark):
    """JVM-only baseline — no Python worker involved."""
    assert spark.range(10).count() == 10


def test_rdd_collect(spark):
    """Driver-side serialization only — no worker process."""
    assert spark.sparkContext.parallelize([1, 2, 3]).collect() == [1, 2, 3]


# --- Worker-dependent (crash on Python 3.12+ Windows) ---

def test_rdd_map(spark):
    """Requires Python worker process — triggers createSimpleWorker on Windows."""
    result = spark.sparkContext.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()
    assert result == [2, 4, 6]


def test_create_dataframe(spark):
    """Requires Python worker — the canonical SPARK-53759 failure case."""
    df = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["name", "age"])
    assert df.count() == 2


def test_udf(spark):
    """UDF execution requires Python worker."""
    str_udf = udf(lambda x: f"val_{x}", StringType())
    rows = spark.range(5).withColumn("x", str_udf("id")).collect()
    assert len(rows) == 5


# --- Python Data Source API (Spark 4.0+) ---


def _has_datasource_api():
    try:
        from pyspark.sql.datasource import DataSource, DataSourceReader  # noqa: F401

        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _has_datasource_api(), reason="DataSource API requires PySpark 4.0+")
def test_datasource_read(spark):
    """Python Data Source read — exercises create_data_source + plan_data_source_read workers."""
    from pyspark.sql.datasource import DataSource, DataSourceReader

    class TestReader(DataSourceReader):
        def read(self, partition):
            yield (0, "a")
            yield (1, "b")

    class TestDataSource(DataSource):
        @classmethod
        def name(cls):
            return "test_53759"

        def schema(self):
            return "id INT, value STRING"

        def reader(self, schema):
            return TestReader()

    spark.dataSource.register(TestDataSource)
    rows = spark.read.format("test_53759").load().collect()
    assert len(rows) == 2
