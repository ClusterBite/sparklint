from delta.tables import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# `DeltaTable` from a star import is still delta-spark's class.
updates = spark.read.parquet("/updates")
lake = DeltaTable.forPath(spark, "/lake/trips")
lake.delete("pickup < '2020-01-01'")  # expect: ACT001
lake.vacuum(0)  # expect: ACT001
(
    DeltaTable.forName(spark, "gold").alias("t")
    .merge(updates.alias("s"), "s.id = t.id")
    .whenMatchedDelete("s.op = 'delete'")
    .whenNotMatchedInsertAll()
    .execute()  # expect: ACT001
)
