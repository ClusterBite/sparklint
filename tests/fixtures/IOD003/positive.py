from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.coalesce(1).write.format("delta").save("/p")  # expect: IOD003
df.repartition(1).write.mode("overwrite").csv("/export")  # expect: IOD003
df.coalesce(1).select("a").writeTo("cat.db.t").append()  # expect: IOD003
one = df.coalesce(1)  # expect: IOD003
one.write.parquet("/x")
small = df.groupBy("k").count().coalesce(1)  # expect: IOD003
small.write.csv("/report")
