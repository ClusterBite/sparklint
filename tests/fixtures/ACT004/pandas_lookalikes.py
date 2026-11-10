import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = pd.read_csv("x.csv")
for idx, row in df.iterrows():
    print(row)
sdf = spark.table("t")
for i, r in sdf.toPandas().iterrows():  # expect: ACT004
    print(r)
