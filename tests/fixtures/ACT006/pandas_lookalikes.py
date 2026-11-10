import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = pd.read_csv("x.csv")
if df.count() > 0:
    pass
if len(df) == 0:
    pass
nums = [1, 2]
if nums.count(1) == 0:
    pass
