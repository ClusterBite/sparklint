from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
a = df.withColumn("d", F.regexp_extract("s", r"(\d+)", 1))
b = "line\n\ttab \\d"
c = r"\bword\b"
d = b"\x00\n"
e = "\u00e9 \N{BULLET}"
