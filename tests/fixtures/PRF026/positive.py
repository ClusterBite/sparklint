from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# Three distinct aggregates over different columns.
a = df.groupBy("day").agg(  # expect: PRF026
    F.countDistinct("user_id"),
    F.countDistinct("session_id"),
    F.count_distinct(F.col("device_id")),
)

# Mixed spellings, sum_distinct, imported name, plus regular aggregates.
b = df.groupBy("day", "country").agg(  # expect: PRF026
    countDistinct("user_id").alias("users"),
    F.sum_distinct("amount").alias("amounts"),
    F.count_distinct(df.device_id),
    F.sum("amount"),
)

# Global aggregate without groupBy.
c = df.agg(F.countDistinct("a"), F.countDistinct("b"), F.countDistinct(df["c"]))  # expect: PRF026

# select() with distinct aggregates (a global aggregation).
d = df.select(F.countDistinct("a"), F.countDistinct("b"), F.countDistinct("c"))  # expect: PRF026

# Wrapped in other expressions.
e = df.groupBy("k").agg(  # expect: PRF026
    (F.countDistinct("a") / F.count("*")).alias("ratio_a"),
    F.coalesce(F.countDistinct("b"), F.lit(0)),
    F.when(F.countDistinct("c") > 1, 1).otherwise(0),
)

# F.expr strings and selectExpr.
f = df.groupBy("k").agg(  # expect: PRF026
    F.expr("count(distinct a)"),
    F.expr("COUNT(DISTINCT b) AS bs"),
    F.countDistinct("c"),
)
g = df.selectExpr("count(distinct a)", "count(distinct b)", "sum(distinct c)")  # expect: PRF026

# pivot results are GroupedData too.
h = df.groupBy("k").pivot("p", ["x", "y"]).agg(  # expect: PRF026
    F.countDistinct("a"), F.countDistinct("b"), F.countDistinct("c")
)

# spark.sql SELECT lists.
q1 = spark.sql(  # expect: PRF026
    """
    SELECT day,
           COUNT(DISTINCT user_id),
           COUNT(DISTINCT session_id),
           COUNT(DISTINCT `device_id`)
    FROM events GROUP BY day
    """
)
QUERY = "select k, count(distinct a), count(distinct b), sum(distinct c) from t group by k"
q2 = spark.sql(QUERY)  # expect: PRF026
table = "events"
q3 = spark.sql(f"SELECT count(distinct a), count(distinct b), count(distinct a, c) FROM {table}")  # expect: PRF026


# Typed parameter.
def summarise(frame: DataFrame) -> DataFrame:
    return frame.groupBy("k").agg(  # expect: PRF026
        F.countDistinct("a"), F.countDistinct("b"), F.countDistinct("c"), F.countDistinct("d")
    )


# Loop.
for col in ["x", "y"]:
    df.groupBy(col).agg(  # expect: PRF026
        F.countDistinct("a"), F.countDistinct("b"), F.countDistinct("c")
    ).show()
