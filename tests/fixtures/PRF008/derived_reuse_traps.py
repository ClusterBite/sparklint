from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# The derived frame feeds one action: still one use.
def summary_collected_once(path: str) -> None:
    data = spark.read.parquet(path).cache()  # expect: PRF008
    summary = data.agg(F.max("x").alias("hi"))
    print(summary.collect()[0]["hi"])


# Two actions, but on different branches: one runs.
def branch_actions(path: str, verbose: bool) -> None:
    data = spark.read.parquet(path).cache()  # expect: PRF008
    summary = data.groupBy("k").count()
    if verbose:
        summary.show()
    else:
        summary.write.parquet("/out/s")


# The derived frame is cached itself: its reuse does not need this cache.
def recached_downstream(path: str) -> None:
    data = spark.read.parquet(path).cache()  # expect: PRF008
    keyed = data.select("k", "x").cache()
    keyed.count()
    keyed.show()
    keyed.unpersist()
    data.unpersist()


# A local frame (not a parameter) cached in place, read once.
def local_marker(path: str) -> None:
    df = spark.read.parquet(path)
    df.cache()  # expect: PRF008
    df.show()


# A parameter cached under a new local name and read once: a local cache.
def rebound_param(df: DataFrame) -> None:
    cached = df.cache()  # expect: PRF008
    cached.show()
