from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("events")

# Direct forms: the DataFrame has the same method.
it = df.rdd.toLocalIterator()  # expect: ACT011 ~"`df.toLocalIterator()` returns the same rows"
head = df.rdd.first()  # expect: ACT011 ~"`df.first()` returns the same rows"
counts = df.groupBy("k").count().rdd.collect()  # expect: ACT011

# RDD steps before the action run Python workers.
lookup = df.rdd.map(lambda r: (r.k, r.v)).collectAsMap()  # expect: ACT011 ~"runs the RDD steps"
best = df.rdd.map(lambda r: r.v).top(3)  # expect: ACT011 ~"runs the RDD steps"
low = df.rdd.map(lambda r: r.v).takeOrdered(3)  # expect: ACT011
some = df.rdd.map(lambda r: r.v).takeSample(False, 10)  # expect: ACT011
sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()  # expect: ACT011
keys = df.rdd.keys().collect()  # expect: ACT011
indexed = df.rdd.zipWithIndex().collect()  # expect: ACT011


def load(events_df):
    return events_df.rdd.collect()  # expect: ACT011 ~"`events_df.collect()`"


# Traps.
# No RDD step and no DataFrame method of that name: no Python worker runs (DBX002 still
# reports the .rdd access).
as_map = df.rdd.collectAsMap()
top3 = df.rdd.top(3)
# Frames that fit on the driver cost nothing either way.
preview = df.limit(10).rdd.collect()
tables = spark.sql("SHOW TABLES").rdd.collect()
local = spark.createDataFrame([(1,)], ["a"]).rdd.map(lambda r: r.a).collect()
# Other RDD actions and RDDs not built from a DataFrame.
n = df.rdd.count()
nums = spark.sparkContext.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()
lines = spark.sparkContext.textFile("/data/x.txt").take(3)
# An RDD held in a name is not followed.
r = df.rdd
held = r.collect()
