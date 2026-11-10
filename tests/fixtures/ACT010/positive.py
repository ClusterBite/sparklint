from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
df.show()  # expect: ACT010
df.show(50)  # expect: ACT010
df.printSchema()  # expect: ACT010
df.explain()  # expect: ACT010
