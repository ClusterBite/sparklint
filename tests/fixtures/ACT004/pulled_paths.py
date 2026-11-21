import pyspark.pandas as ps
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.table("events")
psdf = ps.read_table("events")

# A slice of the pulled rows, a reversed() wrap.
for r in df.collect()[1:]:  # expect: ACT004 ~"for row in df.collect()[1:]"
    print(r)
for r in reversed(df.collect()):  # expect: ACT004 ~"for row in df.collect()"
    print(r)

# Frame rows through a name bound to the pull.
table = df.toArrow()
for rec in table.to_pylist():  # expect: ACT004 ~"table.to_pylist()"
    print(rec)

# Traps: values derived from the rows (the comprehension itself iterates them), a few
# rows of a pull, a dict pulled from an RDD.
ids = [r.id for r in df.collect()]  # expect: ACT004
for i in ids:
    print(i)
rows = df.collect()
for r in rows[:3]:
    print(r)
for k in df.rdd.collectAsMap():
    print(k)
