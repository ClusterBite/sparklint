from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
# A starred literal list or a tuple passes its columns one by one.
a = df.cube(*["a", "b", "c", "d", "e"]).count()  # expect: PRF024
b = df.cube(("a", "b", "c", "d", "e")).count()  # expect: PRF024
# `*cols` of unknown length: no count.
cols = ["a", "b", "c", "d", "e"]
c = df.cube(*cols).count()
