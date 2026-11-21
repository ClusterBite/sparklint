from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
APPEND = "append"


def mode_in_constant(df: DataFrame, parts):
    for p in parts:
        df.filter(F.col("p") == p).write.mode(APPEND).saveAsTable("cat.db.t")  # expect: IOD006


def none_mode_keeps_append(df: DataFrame, parts):
    for p in parts:
        df.filter(F.col("p") == p).write.mode("append").saveAsTable("cat.db.t", mode=None)  # expect: IOD006
