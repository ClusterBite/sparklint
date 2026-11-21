from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")
df.coalesce(8).write.parquet("/p")
df.coalesce(1).groupBy("a").count().write.parquet("/p2")
x = df.coalesce(1)
x.count()
s = "abc"
s.coalesce = None

# A shuffle or combine after the coalesce sets the output partitions (the
# transformation table's shuffle / combine rows, not a list of its own).
o = spark.table("o")
df.coalesce(1).intersect(o).write.parquet("/p3")
df.coalesce(1).exceptAll(o).write.parquet("/p4")
df.coalesce(1).rollup("a").count().write.parquet("/p5")
df.coalesce(1).drop_duplicates(["a"]).write.parquet("/p6")
