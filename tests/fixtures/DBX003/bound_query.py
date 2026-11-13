from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")

# The query a name bound once to the text runs, and the sqlQuery= keyword.
q = "SELECT *, input_file_name() AS src FROM json.`/landing`"
a = spark.sql(q)  # expect: DBX003
b = spark.sql(sqlQuery="SELECT input_file_name() FROM t")  # expect: DBX003
cond = "input_file_name() LIKE '%2024%'"
c = df.filter(cond)  # expect: DBX003
e = df.select(F.expr(cond))  # expect: DBX003

# EXPLAIN only plans the query: nothing runs.
p = spark.sql("EXPLAIN SELECT input_file_name() FROM t").collect()
plan_q = "EXPLAIN FORMATTED SELECT input_file_name() FROM t"
p2 = spark.sql(plan_q).collect()
