# What a loop's action depends on (sparklint_semantic::loop_character depends_on):
# a list literal filled in place is no small literal; a helper argument computed from
# the loop variable in the body is a different source per item.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("cat.db.events")


def load(name):
    return spark.table(name)


def appended_list(days):
    regions = []
    for d in days:
        regions.append(d)
    for r in regions:
        df.filter(F.col("r") == r).count()  # expect: ACT005[warning]


def filled_dict(days):
    by_day = {}
    for d in days:
        by_day[d] = d
    for d in by_day:
        df.filter(F.col("d") == d).count()  # expect: ACT005[warning]


def untouched_list():
    regions = ["eu", "us"]
    for r in regions:
        df.filter(F.col("r") == r).count()  # expect: ACT005[info]


def helper_argument_from_loop_variable(tables):
    for t in tables:
        name = f"cat.db.{t}"
        load(name).count()  # expect: ACT005[info]


def helper_argument_fixed(tables):
    for t in tables:
        name = "cat.db.events"
        load(name).filter(F.col("t") == t).count()  # expect: ACT005[warning]
