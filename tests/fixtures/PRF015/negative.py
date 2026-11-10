from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.groupBy("k").agg(F.collect_set("v"))
b = df.select(F.array_distinct("arr"))


def array_distinct(x):
    return x


def collect_list(x):
    return x


c = array_distinct(collect_list([1, 1]))
