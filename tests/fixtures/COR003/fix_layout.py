from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
# Only parentheses are inserted: comments, line breaks and existing parentheses stay.
a = df.filter(F.col("a") > 1 &  # expect: COR003
              F.col("b") < 2)
b = df.filter(F.col("a") > 1 & (F.col("b") < 2))  # expect: COR003
c = df.filter((F.col("a") > 1) & F.col("b") < 2)  # expect: COR003
d = df.filter(F.col("a") > (1) & F.col("b") < 2)  # expect: COR003
e = df.filter(F.col("a") > 1 & 2 < F.col("b"))  # expect: COR003
f = df.filter(F.col("a") == 1 | F.col("b") == 2 | F.col("c") == 3)  # expect: COR003
