from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
a = df.filter(F.col("a") == None)  # expect: COR001
b = df.filter(df.b != None)  # expect: COR001
c = df.filter(df["c"] == None)  # expect: COR001
d = F.col("d") is None  # expect: COR001
e = F.col("a").alias("b") is not None  # expect: COR001
f = F.lit(1) is None  # expect: COR001
