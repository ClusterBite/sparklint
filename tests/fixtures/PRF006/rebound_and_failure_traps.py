from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# Rebound on one branch, but the original frame also feeds its own action first.
def load(path, item_df=None):
    df = spark.read.csv(path, header=True)  # expect: PRF006
    df.count()
    if item_df is not None:
        df = df.join(item_df, "item", "left")
    df.write.saveAsTable("out")


# Rebound in a loop: every pass re-joins and writes.
base = spark.table("a").groupBy("k").count()  # expect: PRF006
for t in tables:
    out = base.join(spark.table(t), "k")
    out.write.saveAsTable(t + "_out")


# Two actions in the assert condition itself (the message is not involved).
def check_groups(df, col_name):
    distinct = df.select("group_id", col_name).distinct()  # expect: PRF006
    assert distinct.count() == distinct.filter(F.col(col_name).isNotNull()).count()


# The action before the raise runs on every call.
def check_empty(spark_df):
    orphans = spark_df.join(spark.table("parents"), "k", "left_anti")  # expect: PRF006
    n = orphans.count()
    orphans.write.saveAsTable("orphans")
    if n:
        raise ValueError(f"{n} orphan rows")
