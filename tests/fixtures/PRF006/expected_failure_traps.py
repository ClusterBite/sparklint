import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# A try probe and a warns block run to the end: both actions count.
def probe_then_write():
    joined = spark.table("a").join(spark.table("b"), "k")  # expect: PRF006
    try:
        joined.count()
    except Exception:
        return
    joined.write.saveAsTable("out")


def test_warns():
    joined = spark.table("a").join(spark.table("b"), "k")  # expect: PRF006
    joined.count()
    with pytest.warns(FutureWarning):
        joined.toPandas()
