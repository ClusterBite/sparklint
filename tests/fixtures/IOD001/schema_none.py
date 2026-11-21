from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
NO_SCHEMA = None

# `schema=None` means "infer", exactly like passing no schema.
a = spark.read.json("/x.json", schema=None)  # expect: IOD001
b = spark.read.csv("/x.csv", None, inferSchema=True)  # expect: IOD001
c = spark.read.load("/x", "json", NO_SCHEMA)  # expect: IOD001

# Traps: a few literal strings on the driver (tests, examples) infer nothing costly.
t1 = spark.read.json(sc.parallelize(['{"a": 1}', '{"a": 2}']))
t2 = spark.read.json(path=sc.parallelize(['{"a": 1}']))
t3 = spark.read.csv(sc.parallelize(["a,b", "1,2"]), header=True, inferSchema=True)
