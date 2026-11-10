from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

a = spark.sql("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)")  # expect: PRF025
b = spark.sql("select * from a where id not in (select id from b where b.ok)")  # expect: PRF025

QUERY = """
SELECT o.*
FROM orders o
WHERE (o.customer_id, o.region)
      NOT IN (
        SELECT customer_id, region FROM blocked
      )
"""
c = spark.sql(QUERY)  # expect: PRF025

table = "events"
d = spark.sql(f"SELECT * FROM {table} WHERE user_id NOT IN (WITH x AS (SELECT u FROM bots) SELECT u FROM x)")  # expect: PRF025
e = spark.sql(sqlQuery="SELECT k FROM t WHERE k NOT IN ((SELECT k FROM s))")  # expect: PRF025


def build(session: SparkSession):
    return session.sql("SELECT * FROM a WHERE x NOT IN (SELECT x FROM b)")  # expect: PRF025


for day in ["2026-01-01", "2026-01-02"]:
    spark.sql(f"SELECT * FROM e WHERE day = '{day}' AND id NOT IN (SELECT id FROM seen)").write.saveAsTable("t")  # expect: PRF025
