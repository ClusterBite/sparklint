from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
items = [1, 2, 2]
if items.count(2) > 0:
    pass
if "abc".count("a") == 0:
    pass
if df.count() > 10:
    pass
n = df.count()
print(f"rows: {n}")
if n == 0:
    pass
if not df.isEmpty():
    pass
if df.limit(1).count() > 0:
    pass
