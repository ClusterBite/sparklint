from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")
n = df.count()  # expect: ACT001
rows = df.limit(5).collect()  # expect: ACT001
first = df.first()  # expect: ACT001
pdf = df.toPandas()  # expect: ACT001
df.write.mode("overwrite").saveAsTable("out")  # expect: ACT001
