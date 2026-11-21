from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
events = spark.table("events")

# Reading the last row or the first n rows of a full collect.
last = events.collect()[-1]  # expect: ACT009 ~"events.collect()[-1] pulls every row to the driver to read the last one" help~"tail(1)"
head = events.collect()[:5]  # expect: ACT009 ~"events.collect()[:5] pulls every row to the driver to keep 5 — use .take(5)" help~"take(n)"
head0 = events.where("x > 1").collect()[0:3]  # expect: ACT009 ~"keep 3"

# A name bound once to the collect and used only here: one finding, no fix.
rows = events.collect()
n = len(rows)  # expect: ACT009 ~"len() computed on the driver from events.collect()"
picked = events.orderBy("ts").collect()
top = picked[0]  # expect: ACT009 ~'events.orderBy("ts").collect()[0] pulls every row'

# Traps: not a first-n / last read, or the frame is row-bounded.
tail2 = events.collect()[1:]
step = events.collect()[:10:2]
second = events.collect()[1]
neg = events.collect()[-2]
var_n = 4
by_var = events.collect()[:var_n]
small = events.limit(3).collect()[-1]
agg = events.agg(F.max("ts")).collect()[:1]
both = events.collect()
print(both[0], len(both))
