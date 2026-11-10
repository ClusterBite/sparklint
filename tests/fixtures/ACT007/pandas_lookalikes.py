import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = pd.read_csv("x.csv")
print(df.count())
print(len(df))
sdf = spark.table("t")
print(sdf.count())  # expect: ACT007
