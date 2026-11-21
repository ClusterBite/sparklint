from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
ONE = 1
df.coalesce(numPartitions=1).write.parquet("/a")  # expect: IOD003
df.coalesce(ONE).write.parquet("/b")  # expect: IOD003
df.repartition(ONE, "k").write.parquet("/c")  # expect: IOD003
df.hint("coalesce", 1).write.parquet("/d")  # expect: IOD003
df.hint("REPARTITION", 1).write.mode("append").saveAsTable("t2")  # expect: IOD003
df.coalesce(1).hint("broadcast").write.parquet("/e")  # expect: IOD003
df.coalesce(1).filter("a > 1").withColumn("x", F.lit(1)).write.json("/f")  # expect: IOD003
df.coalesce(1).write.jdbc("jdbc:postgresql://h/db", "t")  # expect: IOD003

# Not reported: another partition count, a partitioning step after it, a count that
# differs by branch, rebalance hints, streaming writes, no write.
df.hint("coalesce", 8).write.parquet("/g")
df.coalesce(1).hint("repartition", 8).write.parquet("/h")
df.coalesce(1).orderBy("a").write.parquet("/i")
df.hint("rebalance", 1).write.parquet("/j")
if df.isStreaming:
    n = 1
else:
    n = 4
df.coalesce(n).write.parquet("/k")
df.coalesce(1).writeStream.format("parquet").option("checkpointLocation", "/c").start("/l")
df.coalesce(1).toPandas()
