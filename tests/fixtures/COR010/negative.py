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


# Same frame on both sides: the column order is identical.
minor = spark.table("t").filter(F.col("x") == 1)
oversampled = minor.union(minor)
twice = a.unionAll(a)


class Holder:
    def __init__(self):
        self.df = spark.table("t")

    def doubled(self):
        return self.df.union(self.df)


# The other side is re-selected in the receiver's column order.
extra = b.withColumn("sku", F.lit("ALL"))
aligned = a.union(extra.select(a.columns))
aligned_star = a.union(extra.select(*a.columns))
chained = a.union(extra.select(a.columns)).union(b.select(a.columns))
reversed_side = extra.select(b.columns).union(b)
