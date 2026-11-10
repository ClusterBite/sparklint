from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
import pandas as pd

df = spark.read.parquet("/data")
ok = df.limit(100).toPandas()
agg = df.agg(F.sum("x")).toPandas()
lit = spark.createDataFrame([(1,), (2,)], ["a"]).toPandas()

pdf = pd.read_csv("x.csv")
again = pdf.copy()


class Thing:
    def toPandas(self):
        return 1


t = Thing().toPandas()
