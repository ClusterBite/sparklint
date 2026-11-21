from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Keyword and option forms of the batch checks.
a = spark.read.csv("/x.csv", inferSchema=True)  # expect: IOD001
b = spark.read.csv("/x.csv", header=True, inferSchema=False)  # expect: IOD001
c = spark.read.load("/x", format="csv", inferSchema="true")  # expect: IOD001
d = spark.read.parquet("/x", mergeSchema=True)  # expect: IOD001
e = spark.read.option("mergeSchema", "true").parquet("/x")  # expect: IOD001
f = spark.read.format("orc").option("mergeSchema", True).load("/x")  # expect: IOD001
g = spark.read.xml("/x.xml", rowTag="r")  # expect: IOD001
h = spark.read.json("/x", samplingRatio=0.1)  # expect: IOD001
i = spark.read.json(path="/x")  # expect: IOD001
j = spark.read.options(inferSchema=True, header=True).csv("/x")  # expect: IOD001

# Streaming file sources without a schema (no schemaInference set in this file).
k = spark.readStream.json("/in")  # expect: IOD001
m = spark.readStream.format("csv").option("header", True).load("/in")  # expect: IOD001
n = spark.readStream.format("avro").load("/in")  # expect: IOD001
o = spark.readStream.parquet("/in")  # expect: IOD001

# Traps: no inference cost or a fixed schema.
p = spark.read.format("avro").load("/x")
q = spark.read.text("/x")
r = spark.read.parquet("/x", mergeSchema=False)
s = spark.read.format("parquet").option("mergeSchema", "false").load("/x")
t = spark.readStream.text("/in")
u = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/in")
v = spark.readStream.schema("a INT").json("/in")
