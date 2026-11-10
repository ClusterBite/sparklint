from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.filter((F.col("a") > 1) & (F.col("b") < 2))
flags = 3
ok = flags & 1 == 1
mask = (flags & 2) > 0
b = F.col("a") > (1 & flags)
