import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = pd.read_csv("x.csv")
rows = df.values.tolist()
s = {1, 2}.union({3})
pdf = spark.table("t").toPandas()
vals = pdf.collect() if hasattr(pdf, "collect") else None
