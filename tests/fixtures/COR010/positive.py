from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from functools import reduce

a = spark.table("a")
b = spark.table("b")
u = a.union(b)  # expect: COR010
u2 = a.unionAll(b)  # expect: COR010
all_ = reduce(DataFrame.union, [a, b])  # expect: COR010


# Not the same column order by construction.
c = spark.table("c")
other_cols = a.union(b.select(c.columns))  # expect: COR010
reselected = a.select("x", "y").union(a)  # expect: COR010
picked = a.union(b.select(a.columns[:2]))  # expect: COR010
