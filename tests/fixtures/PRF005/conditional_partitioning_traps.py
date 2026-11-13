from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# global on both branches: only the ordering differs
def ranked(df: DataFrame, desc: bool) -> DataFrame:
    spec = Window.orderBy(F.col("ts").desc()) if desc else Window.orderBy("ts")
    return df.withColumn("rn", F.row_number().over(spec))  # expect: PRF005


# an explicitly empty partitionBy is still one partition
whole = Window.partitionBy()


def totals(df: DataFrame) -> DataFrame:
    return df.withColumn("total", F.sum("x").over(whole))  # expect: PRF005


def framed(df: DataFrame) -> DataFrame:
    w = Window.orderBy("ts")
    w = w.rowsBetween(-3, 0)
    return df.withColumn("avg3", F.avg("x").over(w))  # expect: PRF005
