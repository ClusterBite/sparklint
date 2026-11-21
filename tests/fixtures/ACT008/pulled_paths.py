import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
bad = spark.table("bad")
psbad = ps.read_table("bad")

# A slice of collected values, a reversed() wrap, a pandas-on-Spark pull, rows of an
# Arrow table bound to a name.
ids = [r.id for r in bad.collect()]
a1 = df.filter(F.col("id").isin(ids[1:]))  # expect: ACT008
a2 = df.filter(F.col("id").isin(list(reversed([r.id for r in bad.collect()]))))  # expect: ACT008
a3 = df.filter(F.col("id").isin(psbad.to_pandas()["id"].tolist()))  # expect: ACT008
recs = bad.toArrow().to_pylist()
a4 = df.filter(F.col("id").isin([r["id"] for r in recs]))  # expect: ACT008

# Traps: one collected row, a frame's metadata, a few values.
first = bad.collect()[0]
b1 = df.filter(F.col("id").isin(first))
b2 = df.filter(F.col("c").isin(list(bad.toPandas().dtypes)))
b3 = df.filter(F.col("id").isin([r.id for r in bad.limit(5).collect()]))
