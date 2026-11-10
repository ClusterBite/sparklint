import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = pd.read_csv("x.csv")
mask = (df["a"] > 1) and (df["b"] < 2)
