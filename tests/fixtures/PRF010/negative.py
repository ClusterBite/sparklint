from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.repartition("day").write.partitionBy("day").parquet("/out")
df.repartition(F.col("day")).write.partitionBy("day").save("/out2")
df.repartition(400).write.saveAsTable("wide")
df.repartition(10).groupBy("a").count().write.saveAsTable("agg")
x = df.repartition(10)
x.count()
