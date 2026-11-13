import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# The derived frame's second action sits in a body the test expects to raise:
# on the passing path the cache is read once.
def test_bad_format(path: str) -> None:
    data = spark.read.parquet(path).cache()  # expect: PRF008
    summary = data.agg(F.max("x").alias("hi"))
    print(summary.collect()[0]["hi"])
    with pytest.raises(ValueError):
        summary.write.format("nope").save("/tmp/x")
    data.unpersist()


# Direct uses follow the same policy: an error message and a pytest.raises body
# do not read the cache on the passing path.
def validated(path: str, bad: bool) -> None:
    data = spark.read.parquet(path).cache()  # expect: PRF008
    data.write.parquet("/out/v")
    if bad:
        raise ValueError(f"{data.count()} rows rejected")
    data.unpersist()


def test_bad_target(path: str) -> None:
    data = spark.read.parquet(path).persist()  # expect: PRF008
    data.write.parquet("/out/t")
    with pytest.raises(ValueError):
        data.write.format("nope").save("/tmp/x")
    data.unpersist()
