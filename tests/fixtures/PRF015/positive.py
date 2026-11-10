from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.groupBy("k").agg(F.array_distinct(F.collect_list("v")))  # expect: PRF015
b = df.groupBy("k").agg(F.array_distinct(F.collect_set("v")))  # expect: PRF015
