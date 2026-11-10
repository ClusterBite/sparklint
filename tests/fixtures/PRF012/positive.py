from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("orders")
a = df.withColumn("item", F.explode("items")).filter(F.col("country") == "ZA")  # expect: PRF012
b = (
    df.select("id", "country", F.explode("items").alias("item"))  # expect: PRF012
    .where("country = 'ZA'")
)
