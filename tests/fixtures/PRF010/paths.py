from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
N = 200

# The count is read as IOD003 reads it: constants and numPartitions= raise parallelism.
df.repartition(N).write.saveAsTable("a")
df.repartition(numPartitions=200).write.saveAsTable("b")

# A frame bound on two paths that agree: the steps below the write are followed.
if spark.conf.get("mode") == "x":
    out = df.repartition(8)  # expect: PRF010
else:
    out = df.repartition(8)
out.write.saveAsTable("c")

# Paths that disagree: the walk stops at the name.
if spark.conf.get("mode") == "x":
    mixed = df.repartition(8)
else:
    mixed = df.select("a")
mixed.write.saveAsTable("d")
