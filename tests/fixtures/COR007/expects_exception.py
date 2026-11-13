import unittest

import pytest
from pytest import raises
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import AnalysisException

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")


# Classic Spark analyses a transformation eagerly: a discarded select inside a
# try / pytest.raises / assertRaises block probes for the error on purpose.
def has_column(name):
    try:
        df.select(name)
        return True
    except AnalysisException:
        return False


def test_bad_argument():
    with pytest.raises(ValueError):
        df.withColumn("w", F.col("d").cast("hello"))
    with raises(AnalysisException):
        df.select("missing")
    with pytest.raises(ValueError) as excinfo:
        df.withColumn(
            "week_start", F.col("d")
        )
    assert excinfo


class T(unittest.TestCase):
    def test_missing(self):
        with self.assertRaises(AnalysisException):
            df.drop("x").select("missing")
        with self.assertRaisesRegex(AnalysisException, "UNRESOLVED"):
            df.groupBy("missing")
