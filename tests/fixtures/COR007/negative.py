from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
import pandas as pd

df = spark.table("t")
df.cache()
df.createOrReplaceTempView("v")
df.write.saveAsTable("x")
df.show()
df = df.withColumn("x", F.lit(1))
pdf = pd.read_csv("x.csv")
pdf.drop(columns=["a"], inplace=True)
pdf.fillna(0)


def f(df):
    df.drop("a")  # heuristic df: drop is shared with pandas
    df.fillna(0, inplace=True)
