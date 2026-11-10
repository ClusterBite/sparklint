from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
cols = ["a", "b", "c"]
for c in cols:
    df = df.withColumn(c, df[c].cast("string"))  # expect: PRF001

i = 0
while i < len(cols):
    df = df.withColumn(cols[i], df[cols[i]].cast("int"))  # expect: PRF001
    i += 1
