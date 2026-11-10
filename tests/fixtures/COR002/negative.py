from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
a = df.filter((F.col("a") > 1) & (F.col("b") < 2))
x = True
y = x and not False
cols = df.columns and ["a"]
c = df.filter(~F.col("flag"))


def verify(result, data_df, on=None, how=None):
    cols = result.columns
    if not result.empty or len(cols) != 0:
        pass
    if not data_df.empty:
        pass
    if on is None and how is None:
        pass
