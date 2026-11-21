from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("orders")

# The filter condition text through a name.
ZA = "country = 'ZA'"
a = df.withColumn("item", F.explode("items")).filter(ZA)  # expect: PRF012

# Not reported: the condition reads the exploded column.
HAS_ITEM = "item IS NOT NULL"
b = df.withColumn("item", F.explode("items")).filter(HAS_ITEM)
