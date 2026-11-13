import unittest

import pytest
from pytest import raises
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")


# A discarded action inside a try / pytest.raises / assertRaises block forces
# evaluation to provoke the error on purpose: it runs for its exception.
def table_readable():
    try:
        df.count()
        return True
    except AnalysisException:
        return False


def test_missing_grant():
    with pytest.raises(AnalysisException):
        df.collect()
    with raises(AnalysisException, match="PERMISSION"):
        df.select("secret").toPandas()


def test_deprecated_option():
    with pytest.warns(FutureWarning):
        df.toPandas()


class T(unittest.TestCase):
    def test_missing(self):
        with self.assertRaises(AnalysisException):
            df.filter("missing > 0").count()
        with self.assertRaisesRegex(AnalysisException, "UNRESOLVED"):
            df.collect()
