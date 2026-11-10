from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

import pandas as pd

df = spark.read.parquet("/data")
df = df.withColumnsRenamed({c: c.lower() for c in df.columns})
df = df.drop(*["a", "b"])
for t in ["x", "y"]:
    out = df.select("a", "b")  # not loop-carried
    out.write.saveAsTable(t)

pdf = pd.DataFrame({"a": [1]})
for c in ["a"]:
    pdf = pdf.drop(columns=[c])
names = ["a"]
for c in ["b"]:
    names = names.copy()
