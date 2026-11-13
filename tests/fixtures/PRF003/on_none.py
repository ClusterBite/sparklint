from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.table("a")
b = spark.table("b")

# `on=None` is the same as no condition
r1 = a.join(b, on=None)  # expect: PRF003
r2 = a.join(b, None, "inner")  # expect: PRF003
r3 = a.join(b, on=None, how="cross")  # expect: PRF003

# traps: a real condition, a scalar side, a None that is not the condition
r4 = a.join(b, "id", None)
r5 = a.join(b, on="id", how=None)
r6 = a.join(b.agg(F.max("x")), on=None)
