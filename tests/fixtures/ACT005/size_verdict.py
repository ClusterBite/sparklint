import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("events")
pdf = pd.DataFrame({"id": [1, 2, 3]})
local = spark.createDataFrame(pdf)
rows = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
tables = spark.sql("SHOW TABLES IN sales")

# A job per iteration over a frame that fits the driver is a small job: info.
for k in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"]:
    local.filter(F.col("v") == k).count()  # expect: ACT005[info]
    tables.filter(F.col("tableName") == k).collect()  # expect: ACT005[info]
    big.filter(F.col("v") == k).count()  # expect: ACT005[warning]
