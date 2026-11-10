from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

a = spark.sql("SELECT a, b FROM t")
a.write.saveAsTable("x")
b = spark.sql("SELECT * FROM t")
b.select("a").write.saveAsTable("y")
c = spark.sql("SELECT * FROM t").filter("a > 1").groupBy("a").count()
c.show()

d = spark.sql("WITH u AS (SELECT a, b FROM t) SELECT * FROM u")
d.write.saveAsTable("z")
e = spark.sql("SELECT * FROM (SELECT a FROM t) s").collect()
