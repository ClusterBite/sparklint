from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from functools import reduce

a = spark.table("a")
b = spark.table("b")
u = a.union(b)  # expect: COR010
u2 = a.unionAll(b)  # expect: COR010
all_ = reduce(DataFrame.union, [a, b])  # expect: COR010
