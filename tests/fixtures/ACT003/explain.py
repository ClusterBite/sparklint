# EXPLAIN only plans its query and returns the plan as one row of text:
# nothing is scanned, sorted or written, so rules about the query do not apply.
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

plan_pdf = spark.sql("explain select * from t1 cross join t2").toPandas()

# Trap: the query itself.
pdf = spark.sql("select * from t1 cross join t2").toPandas()  # expect: ACT003
