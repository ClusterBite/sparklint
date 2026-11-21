import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("events")
pdf = pd.DataFrame({"id": [1, 2, 3]})
local = spark.createDataFrame(pdf)
rows = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
tables = spark.sql("SHOW TABLES IN sales")


def top(n):
    # A non-literal limit bounds the rows too: the repartition only sets the file count.
    big.limit(n).repartition(4).write.mode("overwrite").parquet("/out/top")
    big.repartition(4).write.mode("overwrite").parquet("/out/all")  # expect: PRF010
