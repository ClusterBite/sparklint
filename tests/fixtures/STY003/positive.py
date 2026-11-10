from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.drop("tmp").select("a", "b")  # expect: STY003
b = df.drop("x", "y").select(F.col("a"), F.col("b").alias("bb"))  # expect: STY003
