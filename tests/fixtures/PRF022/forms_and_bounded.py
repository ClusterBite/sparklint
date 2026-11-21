from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Other spellings of a star select list.
spark.sql("SELECT *, 1 AS one FROM t").write.saveAsTable("a")  # expect: PRF022
spark.sql("SELECT t.* FROM t").write.saveAsTable("b")  # expect: PRF022
spark.sql("SELECT DISTINCT * FROM t").collect()  # expect: PRF022
spark.sql("SELECT a.*, b.x FROM a JOIN b ON a.k = b.k").cache()  # expect: PRF022

# Traps: a few rows only (the column count hardly matters there).
spark.sql("SELECT * FROM t LIMIT 10").collect()
spark.sql("SELECT * FROM t").take(5)
spark.sql("SELECT * FROM t").head(3)
spark.sql("SELECT * FROM t").limit(5).collect()
few = spark.sql("SELECT * FROM t")
few.limit(20).toPandas()

# Traps: the select list is not a star.
spark.sql("SELECT * EXCEPT (payload) FROM t").write.saveAsTable("c")
spark.sql("SELECT count(*) FROM t").collect()
spark.sql("SELECT a, b.* FROM t JOIN b ON t.k = b.k").write.saveAsTable("d")
