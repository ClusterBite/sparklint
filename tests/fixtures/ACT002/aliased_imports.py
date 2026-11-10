from pyspark.sql import SparkSession as SS
import pyspark.sql as psql

session = SS.builder.appName("x").getOrCreate()
frame = session.read.table("t")
data = frame.collect()  # expect: ACT002


def f(x: psql.DataFrame):
    return x.collect()  # expect: ACT002
