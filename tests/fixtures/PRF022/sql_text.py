from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# `SELECT *` only in a comment: the query lists its columns.
a = spark.sql("SELECT a, b FROM t -- was: SELECT * FROM t")
a.write.saveAsTable("x")

# sqlQuery= and a query bound to a name are read like a positional literal.
STAR = "SELECT * FROM t"
b = spark.sql(sqlQuery=STAR)  # expect: PRF022
b.write.saveAsTable("y")
