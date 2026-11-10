from pyspark.sql import SparkSession


def load_events():
    spark = SparkSession.builder.getOrCreate()
    return spark.table("events")


def load_typed(spark: SparkSession):
    return spark.read.format("delta").load("/x")


events = load_events()
rows = events.collect()  # expect: ACT002
more = load_typed(None).collect()  # expect: ACT002
