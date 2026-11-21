from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


def col(name):
    return F.col(name + "_raw")


df = spark.table("t")
# Only pyspark's col names a column: the local helper reads another one.
a = df.agg(  # expect: PRF026
    F.countDistinct("a"), F.countDistinct(col("a")), F.countDistinct("b")
)
# F.col / F.column / df["a"] / df.a name the same column `a`.
b = df.agg(F.countDistinct("a"), F.countDistinct(F.column("a")), F.countDistinct(df.a))
