from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
n = df.count()  # expect: ACT006
if n == 0:
    raise ValueError("empty")
