from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("sales")

wide = df.groupBy("year").pivot("course").sum("earnings")  # expect: PRF023
wide2 = df.groupBy("year").pivot(F.col("course")).agg(F.sum("x"))  # expect: PRF023
wide3 = df.groupBy("year").pivot("course", None).count()  # expect: PRF023
wide4 = df.groupBy("year").pivot(pivot_col="course").count()  # expect: PRF023

grouped = df.groupBy("year", "region")
wide5 = grouped.pivot("course").sum("earnings")  # expect: PRF023


def build(frame: DataFrame):
    g = frame.groupby("k")
    return g.pivot("v").count()  # expect: PRF023


def untyped(frame):
    # Unknown receiver type: not reported (the oracle cannot prove GroupedData).
    return frame.groupby("k").pivot("v").count()


for day in ["2026-01-01", "2026-01-02"]:
    daily = spark.table("events").where(F.col("day") == day)
    daily.groupBy("user").pivot("event_type").count().write.saveAsTable("t")  # expect: PRF023
