import operator

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
raw = spark.read.table("bronze.events")

# RDD origins come from the type checker: shapes the old chain walks did not list.

# Driver-built data on every path: a tiny test RDD, no finding.
a = spark.read.json(spark.createDataFrame([('{"a": 1}',)], ["j"]).rdd.map(lambda r: r.j))
b = spark.read.json(sc.parallelize(c=['{"a": 1}']))
if a:
    docs = ['{"a": 1}']
else:
    docs = ['{"a": 2}', '{"a": 3}']
c = spark.read.json(sc.parallelize(docs).filter(lambda s: s))
d = spark.read.json(raw.limit(100).rdd.map(lambda r: r.payload))

# Raw file text, kept through rows-only steps and spelled other ways.
second = lambda kv: kv[1]
e = spark.read.json(sc.textFile("/landing/a/*.json").filter(lambda l: l.strip()))  # expect: IOD016 help~"spark.read.json(path)"
f = spark.read.json(sc.textFile("/landing/a").union(sc.textFile("/landing/b")))  # expect: IOD016 help~"spark.read.json(path)"
g = spark.read.json(sc.wholeTextFiles("/landing/docs").map(operator.itemgetter(1)))  # expect: IOD016 help~"spark.read.json(path)"
h = spark.read.json(sc.wholeTextFiles("/landing/docs").map(lambda p: p[-1]))  # expect: IOD016 help~"spark.read.json(path)"
i = spark.read.json(sc.wholeTextFiles("/landing/docs").map(f=second))  # expect: IOD016 help~"spark.read.json(path)"
j = spark.read.json(sc.wholeTextFiles("/landing/docs").coalesce(8).values())  # expect: IOD016 help~"spark.read.json(path)"

# Not file text / not driver-sized: the general wording.
k = spark.read.json(sc.wholeTextFiles("/landing/docs").map(lambda kv: kv[0]))  # expect: IOD016 help~"explicit schema (DDL string)"
l = spark.read.json(sc.textFile("/landing/a").map(lambda s: s.lower()))  # expect: IOD016 help~"explicit schema (DDL string)"
rows = []
for x in range(3):
    rows.append('{"a": %d}' % x)
m = spark.read.json(sc.parallelize(rows))  # expect: IOD016 help~"explicit schema (DDL string)"
