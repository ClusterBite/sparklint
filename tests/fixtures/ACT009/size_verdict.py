import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("events")
pdf = pd.DataFrame({"id": [1, 2, 3]})
local = spark.createDataFrame(pdf)
rows = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
tables = spark.sql("SHOW TABLES IN sales")

# Frames that fit the driver: collecting them is fine.
first_local = local.collect()[0]
first_table = tables.collect()[0]
print(len(rows.collect()), max(r.id for r in local.collect()))

# Not driver-sized: still reported.
first_big = local.unionByName(big).collect()[0]  # expect: ACT009
print(len(big.collect()))  # expect: ACT009
