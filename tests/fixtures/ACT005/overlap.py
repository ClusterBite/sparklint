# Overlap fixture: linted alone, this rule fires on every marked line; with ALL or the
# default preset a higher-precedence rule fires at the same site and this finding is
# dropped (sparklint_linter::overlap::PRECEDENCE; checked by the preset pass).
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def per_day(events: DataFrame, days):
    for d in days:
        events.filter(F.col("day") == d).write.mode("append").saveAsTable("cat.db.daily")  # expect: ACT005


def v2(df: DataFrame, parts):
    for p in parts:
        df.where(F.col("p") == p).writeTo("cat.db.t").append()  # expect: ACT005
