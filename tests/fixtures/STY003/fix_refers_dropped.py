from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.table("a")
b = spark.table("b")
j = a.join(b, a.id == b.id, "inner")
# The dropped column is named again by select: no fix (removing the drop changes the result).
s1 = j.drop(b.id).select("id", "x")  # expect: STY003
s2 = j.drop(b.id).select(F.expr("id + 1"))  # expect: STY003
s3 = j.drop("tmp").select(F.col("t.tmp"))  # expect: STY003
# Not referring to a dropped column: fixed.
s4 = a.drop("tmp").select(F.expr("x + 1"))  # expect: STY003
