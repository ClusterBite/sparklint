from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

a = spark.table("a")
b = spark.table("b")
# The other frame as a keyword argument.
u = a.union(other=b)  # expect: COR010
