from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("t")

# input_file_name() inside a string literal or a comment is not a call.
spark.sql("SELECT 'use input_file_name() here' AS hint")
df.selectExpr("_metadata.file_path AS src -- not input_file_name()")
spark.sql("SELECT input_file_name  () AS f FROM t")  # expect: DBX003
