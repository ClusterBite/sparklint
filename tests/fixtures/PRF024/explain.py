# EXPLAIN only plans its query and returns the plan as one row of text:
# nothing is scanned, sorted or written, so rules about the query do not apply.
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql(
    "EXPLAIN EXTENDED SELECT a, b, c, d, e, count(*) FROM t GROUP BY CUBE(a, b, c, d, e)"
).collect()

# Trap: the query itself.
cube = spark.sql(  # expect: PRF024
    "SELECT a, b, c, d, e, count(*) FROM t GROUP BY CUBE(a, b, c, d, e)"
)
