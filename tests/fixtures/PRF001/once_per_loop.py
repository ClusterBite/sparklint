from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
for c in ["a", "b", "c", "d"]:
    df = df.withColumn(c, F.trim(c))  # expect: PRF001
    df = df.withColumn(c + "_len", F.length(c))
    df = df.withColumn(c + "_up", F.upper(c))
