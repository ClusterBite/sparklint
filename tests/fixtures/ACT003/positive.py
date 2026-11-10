from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
pdf = df.toPandas()  # expect: ACT003
small = df.filter(F.col("x") > 1).toPandas()  # expect: ACT003


def to_local(frame: DataFrame):
    return frame.select("a").toPandas()  # expect: ACT003


import pyspark.pandas as ps

psdf = ps.read_parquet("/p")
local = psdf.to_pandas()  # expect: ACT003
