from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# Running array aggregates written as SQL text.
a = df.withColumn("hist", F.expr("collect_list(page) over (partition by user order by ts)"))  # expect: PRF031
b = df.selectExpr("user", "collect_set(page) OVER (PARTITION BY user ORDER BY ts) AS seen")  # expect: PRF031
c = df.selectExpr(["user", "array_agg(page) over (order by ts)"])  # expect: PRF031
d = spark.sql(  # expect: PRF031
    """
    SELECT user,
           collect_list(page) OVER (
               PARTITION BY user ORDER BY ts
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS before
    FROM events
    """
)
RUNNING = "collect_list(page) filter (where page is not null) over (partition by user order by ts rows unbounded preceding)"
e = df.withColumn("hist", F.expr(RUNNING))  # expect: PRF031

# Not running: unordered, whole-partition, bounded, shrinking, interval frames,
# named windows, other aggregates, GROUP BY, text in a string literal.
f = df.withColumn("all", F.expr("collect_list(page) over (partition by user)"))
g = df.selectExpr(
    "collect_list(page) over (partition by user order by ts rows between unbounded preceding and unbounded following)"
)
h = df.withColumn("last5", F.expr("collect_list(page) over (partition by user order by ts rows between 5 preceding and current row)"))
i = df.withColumn("rest", F.expr("collect_list(page) over (partition by user order by ts rows between current row and unbounded following)"))
j = spark.sql(
    "SELECT collect_list(page) OVER (PARTITION BY user ORDER BY ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) FROM events"
)
k = spark.sql("SELECT collect_list(page) OVER w FROM events WINDOW w AS (PARTITION BY user ORDER BY ts)")
m = df.withColumn("total", F.expr("sum(amount) over (partition by user order by ts)"))
n = spark.sql("SELECT user, collect_list(page) FROM events GROUP BY user ORDER BY user")
o = spark.sql("SELECT 'collect_list(page) over (order by ts)' AS note")
p = spark.sql("EXPLAIN SELECT collect_list(page) OVER (ORDER BY ts) FROM events")

# Suppressed.
q = df.withColumn("hist", F.expr("collect_list(page) over (partition by user order by ts)"))  # noqa: PRF031
