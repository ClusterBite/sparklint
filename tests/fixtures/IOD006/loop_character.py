# The shared loop verdict: judged on the loop the write sits in; a literal / constant
# iterable of at most `driver-loop-small-max` (10) items makes it info.
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REGIONS = ("eu", "us")


def constant_list(df: DataFrame):
    for r in REGIONS:
        df.filter(F.col("r") == r).write.mode("append").saveAsTable("cat.db.by_region")  # expect: IOD006[info]


def small_inner_loop(df: DataFrame, days):
    for d in days:
        for r in ["eu", "us"]:
            df.filter((F.col("d") == d) & (F.col("r") == r)).write.mode("append").saveAsTable("cat.db.out")  # expect: IOD006[info]


def small_outer_loop(df: DataFrame, days):
    for r in ["eu", "us"]:
        for d in days:
            df.filter((F.col("d") == d) & (F.col("r") == r)).write.mode("append").saveAsTable("cat.db.out")  # expect: IOD006[warning]
