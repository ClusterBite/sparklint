from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# Read once, by a summary frame that feeds two actions: both reuse the cache.
def summary_shown_and_collected(path: str) -> None:
    data = spark.read.parquet(path).cache()
    summary = data.agg(F.min("x").alias("lo"), F.max("x").alias("hi"))
    summary.show()
    row = summary.collect()[0]
    print(row["lo"], row["hi"])


# Two levels of derived frames, two writes at the end.
def two_writes_downstream(path: str) -> None:
    base = spark.read.parquet(path).persist()
    cleaned = base.filter(F.col("x") > 0)
    cleaned.write.parquet("/out/a")
    cleaned.write.json("/out/b")


# A derived frame used in a loop.
def derived_in_loop(path: str, days: list) -> None:
    events = spark.read.parquet(path).cache()
    keyed = events.select("day", "x")
    for d in days:
        keyed.filter(F.col("day") == d).count()


# A bare cache statement on a parameter caches the caller's DataFrame object
# for the caller, which reads it.
def cache_for_caller(df: DataFrame) -> bool:
    if df.isStreaming:
        return False
    try:
        df.persist()
    except Exception:
        return False
    return True


def cache_param_and_count(df: DataFrame) -> int:
    df.cache()
    return df.count()
