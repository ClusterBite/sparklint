from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
# flatMap flattens what the lambda returns: the column-to-list idiom.
names = df.select("name").rdd.flatMap(lambda x: x).collect()  # expect: DBX002
tags = df.rdd.flatMap(lambda r: r.tags).collect()  # expect: DBX002
# The flattened value gets a name no word of the lambda uses.
vals = df.rdd.flatMap(lambda v: v.value).collect()  # expect: DBX002
either = df.rdd.flatMap(lambda r: r.a if r.b else r.c).collect()  # expect: DBX002
# Not collected: reported, no fix.
first = df.rdd.flatMap(lambda r: r).take(5)  # expect: DBX002
