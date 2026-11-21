from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Raw lines of files read through an RDD instead of spark.read.json(path).
lines = sc.textFile("/landing/events/*.json")
a = spark.read.json(lines)  # expect: IOD016
b = spark.read.json(sc.textFile("/landing/more/*.json"))  # expect: IOD016

# Non-literal parallelize: not the tiny-test exemption.
c = spark.read.json(sc.parallelize(range(10)).map(str))  # expect: IOD016

# Traps: a schema in the reader chain, a direct file read.
t1 = spark.read.option("schema", "a INT").json(lines)
t2 = spark.read.schema("a INT").json(sc.textFile("/landing/x.json"))
t3 = spark.read.json("/landing/events/*.json", schema="a INT")
