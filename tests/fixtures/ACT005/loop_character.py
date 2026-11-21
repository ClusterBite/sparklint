# The shared loop verdict (sparklint_semantic::loop_character): judged on the loop
# the action sits in; a literal / constant iterable of at most
# `driver-loop-small-max` (10) items or a different source per item makes it info.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("cat.db.events")
REGIONS = ["eu", "us", "apac"]


def constant_list():
    for r in REGIONS:
        df.filter(F.col("r") == r).count()  # expect: ACT005[info]


def small_inner_loop(dates):
    for d in dates:
        for r in ["eu", "us"]:
            df.filter((F.col("d") == d) & (F.col("r") == r)).count()  # expect: ACT005[info]


def small_outer_loop(dates):
    for r in ["eu", "us"]:
        for d in dates:
            df.filter((F.col("d") == d) & (F.col("r") == r)).count()  # expect: ACT005[warning]


def sql_predicate_hole(months):
    # The query text changes, the table does not: N scans of one table.
    for m in months:
        spark.sql(f"SELECT * FROM cat.db.sales WHERE m = '{m}'").collect()  # expect: ACT005[warning]


def sql_table_hole(tables):
    for t in tables:
        spark.sql(f"SELECT * FROM cat.db.{t}").collect()  # expect: ACT005[info]


def joined_per_item(tables):
    base = spark.table("cat.db.keys")
    for t in tables:
        base.join(spark.table(t), "k").count()  # expect: ACT005[info]


def frame_built_per_item(tables):
    base = spark.table("cat.db.keys")
    for t in tables:
        other = spark.table(t)
        base.unionByName(other).count()  # expect: ACT005[info]
