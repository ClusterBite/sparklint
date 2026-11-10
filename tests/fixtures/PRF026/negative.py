import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# Two distinct column sets: below the default threshold of 3.
a = df.groupBy("day").agg(F.countDistinct("user_id"), F.countDistinct("session_id"))

# The same column set several times: one distinct group, no Expand.
b = df.groupBy("day").agg(
    F.countDistinct("user_id"),
    F.count_distinct(F.col("user_id")).alias("again"),
    F.sum_distinct("user_id"),
    F.countDistinct(df.user_id),
)

# One multi-column distinct aggregate counts once; argument order does not matter.
c = df.agg(F.countDistinct("a", "b"), F.countDistinct("b", "a"), F.countDistinct("a"))

# Approximate distinct counts and regular aggregates.
d = df.groupBy("k").agg(
    F.approx_count_distinct("a"),
    F.approx_count_distinct("b"),
    F.approx_count_distinct("c"),
    F.count("d"),
    F.sum("e"),
)

# Distinct aggregates split over several aggregations.
e1 = df.groupBy("k").agg(F.countDistinct("a"), F.countDistinct("b"))
e2 = df.groupBy("k").agg(F.countDistinct("c"))

# select DISTINCT / dropDuplicates are not distinct aggregates.
f = df.select("a", "b", "c").distinct()
q1 = spark.sql("SELECT DISTINCT a, b, c FROM t")

# SQL: two distinct sets; distinct aggregates in strings or comments; split across
# nested SELECTs (one per Aggregate).
q2 = spark.sql("SELECT count(distinct a), count(distinct b), count(a) FROM t")
q3 = spark.sql("SELECT 'count(distinct a), count(distinct b), count(distinct c)' AS doc FROM t")
q4 = spark.sql(
    """
    SELECT count(DISTINCT x), count(DISTINCT y)  -- count(distinct z)
    FROM (SELECT count(distinct a) AS x, count(distinct b) AS y FROM t GROUP BY k)
    """
)

# Unknown column lists are not counted.
exprs = [F.countDistinct(c) for c in ["a", "b", "c"]]
g = df.groupBy("k").agg(*exprs)

# pandas and user objects with look-alike names.
pdf = pd.DataFrame({"a": [1]})
h = pdf.agg({"a": "nunique"})


class Stats:
    def agg(self, *cols):
        return cols


s = Stats().agg(F.countDistinct("a"), F.countDistinct("b"), F.countDistinct("c"))

# Explicit opt-out.
i = df.agg(F.countDistinct("a"), F.countDistinct("b"), F.countDistinct("c"))  # noqa: PRF026
