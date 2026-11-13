from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
fmt = spark.conf.get("fmt", "csv")

# A reader built on several paths, each a fresh `spark.read`: no path sets a schema.
if fmt == "csv":
    r = spark.read.format("csv")
else:
    r = spark.read.format("csv").option("sep", ";")
a = r.load("/x")  # expect: IOD001

r1 = spark.read.option("header", True) if fmt == "csv" else spark.read.option("sep", ";")
b = r1.csv("/x")  # expect: IOD001

# A schema on one path: not reported.
if fmt == "wide":
    r2 = spark.read.format("csv").schema("a INT")
else:
    r2 = spark.read.format("csv")
c = r2.load("/x")

# The paths disagree on the format: unknown, not reported.
r3 = spark.read.format("csv") if fmt == "csv" else spark.read.format("parquet")
d = r3.load("/x")
