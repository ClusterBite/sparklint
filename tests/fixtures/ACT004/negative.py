from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
for c in df.columns:
    print(c)
for name, dtype in df.dtypes:
    print(name, dtype)
for r in df.limit(10).collect():
    print(r)
my_list = [1, 2, 3]
for x in my_list:
    print(x)
