import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# A body a test expects to raise never completes: its actions are not reuse.
def test_bad_write():
    joined = spark.table("a").join(spark.table("b"), "k")
    joined.count()
    with pytest.raises(ValueError):
        joined.write.format("nope").save("/tmp/x")


def test_assert_message():
    agg = spark.table("a").groupBy("k").count()
    assert agg.count() > 0, agg.collect()
