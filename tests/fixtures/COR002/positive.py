from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
a = df.filter((F.col("a") > 1) and (F.col("b") < 2))  # expect: COR002
b = df.filter(F.col("a").isNull() or F.col("b").isNull())  # expect: COR002
c = df.filter(not F.col("flag"))  # expect: COR002
d = df.filter(df.a.isNull() and df.b > 1)  # expect: COR002
