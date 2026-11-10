from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

import pandas as pd

df = spark.read.parquet("/data")
df2 = df.filter(F.col("a") > 1).select("a").distinct()
items = [1, 2, 2]
items.count(2)
"a,b".count(",")
pdf = pd.read_csv("x.csv")
pdf.head(5)
",".join(["a", "b"])
