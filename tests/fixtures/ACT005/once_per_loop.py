import time

from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()


def retry_count(df: DataFrame):
    # Retry loop: the job completes once, then the loop ends.
    for attempt in range(3):
        try:
            n = df.count()
            break
        except Exception:
            time.sleep(2 ** attempt)
    return n


def first_existing(tables):
    for t in tables:
        if spark.catalog.tableExists(t):
            return spark.table(t).count()


def retry_overwrite(df: DataFrame, path):
    while True:
        try:
            df.write.mode("overwrite").parquet(path)
            break
        except Exception:
            time.sleep(5)


def count_until_done(df: DataFrame, parts):
    for p in parts:
        n = df.filter(df.p == p).count()  # expect: ACT005
        if n > 0:
            break
