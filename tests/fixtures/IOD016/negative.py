import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
raw = spark.read.table("bronze.events")

# file paths: schema inference is IOD001 territory, not this rule
a = spark.read.json("/landing/events/*.json")
b = spark.read.json(["/a.json", "/b.json"])

# explicit schema: no inference job
c = spark.read.json(raw.rdd.map(lambda r: r.payload), schema="id INT, name STRING")
d = spark.read.schema("id INT").json(raw.rdd.map(lambda r: r.payload))

# tiny RDD of literal strings (tests, examples)
e = spark.read.json(sc.parallelize(['{"a": 1}', '{"a": 2}']))
f = spark.read.json(spark.sparkContext.parallelize(['{"a": 1}']))
json_strings = ['{"name": "Yin"}']
other_rdd = sc.parallelize(json_strings)
f2 = spark.read.json(other_rdd)

# the recommended forms
g = raw.select(F.schema_of_json(F.lit('{"a": 1}')))
h = raw.withColumn("j", F.from_json("payload", "a INT"))

# not a Spark reader
i = json.loads('{"a": 1}')


class Reader:
    def json(self, x):
        return x


r = Reader()
k = r.json(raw.rdd.map(lambda x: x))

# unknown argument type
def load(thing):
    return spark.read.json(thing)


# noqa
m = spark.read.json(raw.rdd.map(lambda r: r.payload))  # noqa: IOD016
