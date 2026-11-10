from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
cols = get_cols()
for c in cols:
    df = df.withColumnRenamed(c, c.lower())  # expect: PRF002

for c in cols:
    df = df.drop(c)  # expect: PRF002

for c in cols:
    df = df.na.fill(0, subset=[c])  # expect: PRF002
    df = df.select("*", F.lit(1).alias(c))
