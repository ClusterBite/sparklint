# EXPLAIN only plans its query and returns the plan as one row of text:
# nothing is scanned, sorted or written, so rules about the query do not apply.
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

plan = spark.sql("EXPLAIN FORMATTED SELECT * FROM big ORDER BY rand()").collect()
spark.sql("explain extended select a from t order by rand() limit 10").show()

# Trap: the query itself.
sample = spark.sql("SELECT a FROM t ORDER BY rand() LIMIT 10")  # expect: PRF027
