from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The read is only asked for its schema: inference is the point (the schema then
# feeds a streaming reader), there is no extra scan to save.
input_schema = spark.read.format("json").option("multiline", "true").load("/in").schema
stream = spark.readStream.schema(input_schema).json("/in")

json_schema = spark.read.json("/sample").schema.json()
header = spark.read.option("header", True).csv("/x.csv").columns
types = spark.read.csv("/x.csv", header=True, inferSchema=True).dtypes
spark.read.json("/sample").printSchema()
merged = spark.read.option("mergeSchema", "true").parquet("/p").schema
