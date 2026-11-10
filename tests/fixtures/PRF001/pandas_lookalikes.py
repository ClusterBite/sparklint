import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = pd.read_csv("x.csv")
for c in ["a", "b"]:
    df = df.assign(**{c: 1})
sdf = spark.table("t")
for c in ["a", "b"]:
    sdf = sdf.withColumn(c, sdf[c] + 1)  # expect: PRF001
