from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("orders")
a = df.withColumn("item", F.explode("items")).filter(F.col("item") > 1)
b = df.select("id", F.explode("items").alias("item")).where("item > 1")
c = df.filter(F.col("country") == "ZA").withColumn("item", F.explode("items"))
d = df.withColumn("item", F.explode("items")).withColumn("p", F.col("item.price")).filter(F.col("p") > 3)
e = df.select(F.explode("items")).filter(F.col("col") > 1)
name = "item"
f = df.withColumn("item", F.explode("items")).filter(F.col(name) > 1)
