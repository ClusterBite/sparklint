from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")

# A count plus partition columns that are all write partition columns: bounded
# files per output partition, intentional.
df.repartition(8, "day").write.partitionBy("day").saveAsTable("a")
df.repartition(8, F.col("day")).write.partitionBy("day", "hour").saveAsTable("b")
df.repartition(16, "day").writeTo("c").partitionedBy(F.col("day")).create()

# Traps: a column outside partitionBy, or an expression we cannot name.
df.repartition(8, "day", "id").write.partitionBy("day").saveAsTable("d")  # expect: PRF010
df.repartition(8, F.hash("day")).write.partitionBy("day").saveAsTable("e")  # expect: PRF010
df.repartition(8).write.partitionBy("day").saveAsTable("f")  # expect: PRF010
