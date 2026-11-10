from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.coalesce(8).write.parquet("/p")
df.coalesce(1).groupBy("a").count().write.parquet("/p2")
x = df.coalesce(1)
x.count()
s = "abc"
s.coalesce = None
