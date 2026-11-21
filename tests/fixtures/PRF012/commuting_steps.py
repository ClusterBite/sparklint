from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("orders")

# Steps the filter commutes with still leave it after the explode.
a = df.withColumn("i", F.explode("items")).drop("items").distinct().filter(F.col("country") == "ZA")  # expect: PRF012
b = df.withColumn("i", F.explode("items")).orderBy("id").filter(F.col("country") == "ZA")  # expect: PRF012
c = df.withColumn("i", F.explode("items")).dropDuplicates().filter(F.col("country") == "ZA")  # expect: PRF012
# A later column that does not read the exploded value.
d = df.withColumn("i", F.explode("items")).withColumn("u", F.expr("upper(country)")).filter(F.col("u") == "ZA")  # expect: PRF012
