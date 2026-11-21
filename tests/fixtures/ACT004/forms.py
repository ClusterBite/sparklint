from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")

# toLocalIterator with its prefetch argument still pulls every row.
for r in df.toLocalIterator(True):  # expect: ACT004
    print(r)
for r in df.toLocalIterator(prefetchPartitions=True):  # expect: ACT004
    print(r)

# One dict per row through pandas or Arrow.
for rec in df.toPandas().to_dict("records"):  # expect: ACT004
    print(rec)
for rec in df.toPandas().to_dict(orient="records"):  # expect: ACT004
    print(rec)
names = [rec["a"] for rec in df.toArrow().to_pylist()]  # expect: ACT004

# Rows held in a name bound to collect() / toLocalIterator().
rows = df.collect()
for r in rows:  # expect: ACT004
    print(r)
it = df.toLocalIterator()
for r in it:  # expect: ACT004
    print(r)

# Traps: not one item per row, or bounded.
for k, v in df.toPandas().to_dict().items():
    print(k, v)
for col_name, values in df.toPandas().to_dict("list").items():
    print(col_name, values)
for rec in df.limit(10).toArrow().to_pylist():
    print(rec)
for rec in df.agg(F.max("a")).toPandas().to_dict("records"):
    print(rec)
for rec in spark.sql("SHOW TABLES IN db").toPandas().to_dict("records"):
    print(rec)
