import functools
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
cols = ["a", "b", "c"]
# functools.reduce over withColumn is the loop in another shape.
out = functools.reduce(lambda d, c: d.withColumn(c, F.lit(1)), cols, df)  # expect: PRF001 ~"functools.reduce(...withColumn...) adds one projection per element"
