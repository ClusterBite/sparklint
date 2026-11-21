from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
raw = spark.read.table("bronze.events")
rdd = raw.rdd.map(lambda r: r.payload)
NO_SCHEMA = None

# `schema=None` means "infer", exactly like passing no schema.
a = spark.read.json(rdd, schema=None)  # expect: IOD016
b = spark.read.json(rdd, None)  # expect: IOD016
c = spark.read.json(rdd, schema=NO_SCHEMA)  # expect: IOD016

# Whole-file contents of sc.wholeTextFiles: read the files with multiLine instead.
d = spark.read.json(sc.wholeTextFiles("/landing/docs").values())  # expect: IOD016 help~"multiLine=True"
e = spark.read.json(sc.wholeTextFiles("/landing/docs").map(lambda kv: kv[1]))  # expect: IOD016 help~"multiLine=True"

# Traps: a real schema in a name, a schema that is None on one path only.
SCHEMA = "a INT"
t1 = spark.read.json(rdd, schema=SCHEMA)
maybe = None
if raw.isStreaming:
    maybe = SCHEMA
t2 = spark.read.json(rdd, schema=maybe)
