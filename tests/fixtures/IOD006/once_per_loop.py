import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def retry_write(df: DataFrame):
    # Retry loop: the append succeeds once, then the loop ends.
    for attempt in range(3):
        try:
            df.write.mode("append").saveAsTable("cat.db.t")
            break
        except Exception:
            time.sleep(2 ** attempt)


def retry_while(df: DataFrame):
    while True:
        try:
            df.write.mode("append").saveAsTable("cat.db.t")
        except Exception:
            time.sleep(5)
            continue
        break


def first_match(df: DataFrame, candidates):
    for c in candidates:
        if c.ready:
            df.write.mode("append").saveAsTable("cat.db.t")
            return c


def retry_inside_parts(df: DataFrame, parts):
    # The retry loop runs once per part: the outer loop still appends per part.
    for p in parts:
        for attempt in range(3):
            try:
                df.filter(F.col("p") == p).write.mode("append").saveAsTable("cat.db.t")  # expect: IOD006
                break
            except Exception:
                time.sleep(1)


def conditional_break(df: DataFrame):
    while True:
        df.write.mode("append").saveAsTable("cat.db.t")  # expect: IOD006
        if done():
            break


def continue_after_write(df: DataFrame, parts):
    for p in parts:
        df.filter(F.col("p") == p).write.mode("append").saveAsTable("cat.db.t")  # expect: IOD006
        if p.more:
            continue
        break
