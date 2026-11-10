from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.udf.register("clean_name", lambda s: s.strip())
df = spark.table("users")

# The UDF name inside a string literal or a comment is not a call.
a = df.filter("note = 'see clean_name(x)'")
c = df.filter("name != '' -- clean_name(name) is slow")
b = df.filter("clean_name(name) != ''")  # expect: UDF002
