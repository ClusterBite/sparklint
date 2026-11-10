from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.repartition(10).write.format("delta").saveAsTable("t2")  # expect: PRF010
df.repartition("country").write.partitionBy("day").parquet("/out")  # expect: PRF010
out = df.repartition(8).select("a", "b")  # expect: PRF010
out.write.mode("overwrite").save("/p")
# repartition(1) is IOD003 when that rule is enabled (overlap::PRECEDENCE).
df.repartition(1).write.csv("/one")  # expect: PRF010
