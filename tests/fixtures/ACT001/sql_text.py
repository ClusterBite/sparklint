from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

DDL = "INSERT INTO t SELECT 1"
spark.sql(DDL)  # expect: ACT001
spark.sql("-- refresh the cache\nREFRESH TABLE t")  # expect: ACT001
spark.sql("SELECT 'INSERT' AS kind")
