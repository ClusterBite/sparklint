import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# A try probe and a warns block run to the end: both read the cache.
def probed(path: str) -> None:
    data = spark.read.parquet(path).cache()
    try:
        data.count()
    except Exception:
        return
    data.write.parquet("/out/p")
    data.unpersist()


def test_warns(path: str) -> None:
    data = spark.read.parquet(path).cache()
    data.write.parquet("/out/w")
    with pytest.warns(FutureWarning):
        data.toPandas()
    data.unpersist()
