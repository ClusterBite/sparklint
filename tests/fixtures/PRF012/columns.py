from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
# The filter reads `country` only: a string value or F.lit("tag") is not the column.
a = df.withColumn("tag", F.explode("tags")).filter("country = 'tag'")  # expect: PRF012
b = df.withColumn("tag", F.explode("tags")).filter(  # expect: PRF012
    F.col("country") == F.lit("tag")
)
# A dotted name reads each of its parts (over-inclusive): `t.tag` may be the exploded column.
c = df.withColumn("tag", F.explode("tags")).filter(F.col("t.tag") > 1)
# An f-string condition could name any column.
name = "tag"
d = df.withColumn("tag", F.explode("tags")).filter(F.expr(f"{name} > 1"))
