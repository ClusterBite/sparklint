from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# GROUP BY CUBE inside a string literal.
spark.sql("SELECT 'GROUP BY CUBE(a, b, c, d, e)' AS note FROM t")

spark.sql(sqlQuery="SELECT a FROM t GROUP BY CUBE(a, b, c, d, e)")  # expect: PRF024
