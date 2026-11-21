from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
events = spark.table("events")
flag = True

# Other pulls of every row: toLocalIterator, toPandas, toArrow.
mx = max(r.ts for r in events.toLocalIterator())  # expect: ACT009 ~"max() computed on the driver from events.toLocalIterator()"
n_rows = len(events.toPandas())  # expect: ACT009 ~"len() computed on the driver from events.toPandas()"
total = sum(events.toPandas()["amount"])  # expect: ACT009 ~"sum() computed on the driver from events.toPandas()"
n_arrow = len(events.toArrow())  # expect: ACT009
n_iter = len(list(events.toLocalIterator()))  # expect: ACT009

# Through names: every binding, each read only here.
if flag:
    picked = events.collect()
else:
    picked = events.where("x > 1").collect()
n_picked = len(picked)  # expect: ACT009 ~"len() computed on the driver from events.collect()"
rows = events.select("id").collect()
ids = [r.id for r in rows]
distinct = set(ids)  # expect: ACT009 ~'set() computed on the driver from events.select("id").collect()'
pdf = events.toPandas()
top = max(pdf["amount"])  # expect: ACT009
if flag:
    latest = events.orderBy("ts").collect()
else:
    latest = events.orderBy("id").collect()
head = latest[0]  # expect: ACT009 ~'events.orderBy("ts").collect()[0] pulls every row'

# One finding per pull: the innermost aggregate.
n_distinct = len(set(r.id for r in events.collect()))  # expect: ACT009 ~"set() computed"

# Traps: an iterator has no len(), a frame's other aggregates walk its column labels,
# rows also used elsewhere, an iterator is not indexable, frames that fit the driver.
it = events.toLocalIterator()
print(len(it))
labels = sorted(events.toPandas())
kept = events.collect()
print(len(kept), kept[0])
first_iter = list(events.toLocalIterator())[0]
small = len(spark.createDataFrame([(1,), (2,)], ["a"]).toPandas())
few = max(r.ts for r in events.limit(10).toLocalIterator())
