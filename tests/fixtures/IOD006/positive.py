from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
TABLE = "cat.db.events"


def per_day(events: DataFrame, days):
    for d in days:
        events.filter(F.col("day") == d).write.mode("append").saveAsTable("cat.db.daily")  # expect: IOD006


def per_key(df: DataFrame):
    keys = [r.k for r in df.select("k").distinct().collect()]
    for k in keys:
        out = df.filter(F.col("k") == k).groupBy("v").count()
        out.write.format("delta").mode("append").save("/mnt/out/counts")  # expect: IOD006


def keyword_mode(df: DataFrame, parts):
    for p in parts:
        df.filter(F.col("p") == p).write.saveAsTable(TABLE, mode="append")  # expect: IOD006


def insert_into(df: DataFrame, parts):
    for p in parts:
        df.filter(F.col("p") == p).write.insertInto(TABLE)  # expect: IOD006


def v2_append(df: DataFrame, parts):
    for p in parts:
        df.where(F.col("p") == p).writeTo(TABLE).append()  # expect: IOD006


def option_path(df: DataFrame, parts):
    for p in parts:
        df.where(F.col("p") == p).write.mode("append").option("path", "/mnt/x").format("delta").save()  # expect: IOD006


def while_poll(df: DataFrame):
    while more_batches():
        batch = df.where(F.col("seen") == F.lit(False))
        batch.write.mode("append").parquet("/mnt/landing")  # expect: IOD006


def small_literal(df: DataFrame):
    for region in ["eu", "us"]:
        df.where(F.col("region") == region).write.mode("append").saveAsTable("cat.db.all_regions")  # expect: IOD006


def positional_mode(df: DataFrame):
    for i in range(3):
        df.write.save("/mnt/out", "delta", "append")  # expect: IOD006
