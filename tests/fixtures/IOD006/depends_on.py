# What the write target depends on (sparklint_semantic::loop_character depends_on):
# a target rebound to the same constant on every iteration is one target; one computed
# from the loop variable in the body is a target per item.
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def same_target_rebound(df: DataFrame, days):
    for d in days:
        target = "cat.db.daily"
        df.filter(F.col("d") == d).write.mode("append").saveAsTable(target)  # expect: IOD006


def target_per_item(df: DataFrame, days):
    for d in days:
        target = f"cat.db.daily_{d}"
        df.filter(F.col("d") == d).write.mode("append").saveAsTable(target)
