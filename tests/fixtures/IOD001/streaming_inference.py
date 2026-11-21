from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# The session infers streaming schemas: the query starts, so a warning instead of an error.
spark.conf.set("spark.sql.streaming.schemaInference", "true")

k = spark.readStream.json("/in")  # expect: IOD001
m = spark.readStream.format("csv").option("header", True).load("/in")  # expect: IOD001
t = spark.readStream.text("/in")
v = spark.readStream.schema("a INT").json("/in")
