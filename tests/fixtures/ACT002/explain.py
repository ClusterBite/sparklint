# EXPLAIN only plans its query and returns the plan as one row of text:
# nothing is scanned, sorted or written, so rules about the query do not apply.
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

plan_rows = spark.sql("EXPLAIN FORMATTED SELECT * FROM big").collect()
plan_line = spark.sql("explain cost select * from big limit 100000000").collect()[0][0]
query = "EXPLAIN SELECT a FROM big JOIN other USING (k)"
joined_plan = spark.sql(query).collect()

# Trap: the query itself, and a string that only mentions EXPLAIN.
rows = spark.sql("SELECT * FROM big").collect()  # expect: ACT002
notes = spark.sql("SELECT 'EXPLAIN' AS kw, a FROM big").collect()  # expect: ACT002
