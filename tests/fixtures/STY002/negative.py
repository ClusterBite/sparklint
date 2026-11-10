from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
import os

a = spark.table("a")
b = spark.table("b")
j = a.join(b, "id", "left")
j2 = a.join(b, on="id", how="inner")
s = ",".join(["a", "b"])
p = os.path.join("a", "b")
