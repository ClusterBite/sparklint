import dlt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


@dlt.table
def a():
    return dlt.read_stream("events_clean")  # expect: DBX017


@dlt.table
def b():
    return dlt.read_stream("live.events_clean").select("x")


@dlt.table(name="c_tbl")
def c():
    return spark.readStream.table("LIVE.events_clean")


@dlt.table
def d():
    return (
        spark.readStream.format("cloudFiles")  # expect: DBX017
        .option("cloudFiles.format", "json")
        .load("/Volumes/raw/landing/events")
    )


@dlt.table
def e():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/raw/landing/events")


@dlt.table
def f():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/Volumes/raw/landing/events")
