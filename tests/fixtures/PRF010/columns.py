from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")

# partitionBy columns as a tuple or a starred list are read too: intentional.
df.repartition(8, "day").write.partitionBy(("day", "hour")).saveAsTable("a")
df.repartition(8, "day").write.partitionBy(*["day"]).saveAsTable("b")
# `*cols` of unknown names: the subset test cannot pass.
cols = ["day"]
df.repartition(8, "day").write.partitionBy(*cols).saveAsTable("c")  # expect: PRF010
# Partitioning columns read like every other rule reads them: pyspark's `col`,
# `df.a` and `df["a"]` name a column...
df.repartition(F.col("day"), df.hour).write.partitionBy("day", "hour").saveAsTable("d")
df.repartition(df["day"]).write.partitionBy("day").saveAsTable("e")


# ...a local function that happens to be called `col` does not.
def col(name):
    return F.upper(F.col(name))


df.repartition(col("day")).write.partitionBy("day").saveAsTable("f")  # expect: PRF010
