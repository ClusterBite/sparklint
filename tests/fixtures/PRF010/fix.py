from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.repartition(10).write.format("delta").saveAsTable("t2")  # expect: PRF010
