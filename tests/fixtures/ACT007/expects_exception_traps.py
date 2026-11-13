import pytest
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")


def test_after_the_block():
    with pytest.raises(AnalysisException):
        spark.table("missing")
    df.count()  # expect: ACT007


def test_nested_in_the_block(flag):
    # Only statements directly in the body are probes.
    with pytest.raises(AnalysisException):
        if flag:
            df.collect()  # expect: ACT007


def cleanup():
    try:
        pass
    finally:
        df.count()  # expect: ACT007
