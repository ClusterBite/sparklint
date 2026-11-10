from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.filter(F.col("a") > 1 & F.col("b") < 2)  # expect: COR003
b = df.filter(F.col("a") == 1 | F.col("b") == 2)  # expect: COR003
c = df.where(df.x >= 0 & df.y <= 5 & df.z == 3)  # expect: COR003
