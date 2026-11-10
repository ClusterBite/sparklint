from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.table("a")
b = spark.table("b")
u = a.unionByName(b)
s = {1, 2}.union({3})
rdd = spark.sparkContext.parallelize([1]).union(spark.sparkContext.parallelize([2]))


class Wrapper:
    def __init__(self, jdf: "JavaObject"):
        self._jdf: "JavaObject" = jdf

    def union(self, other):
        return Wrapper(self._jdf.union(other._jdf))
