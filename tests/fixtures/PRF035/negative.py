from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")
cols = ["id"]


def score(pdf):
    return pdf


# A real grouping key: one group per key, spread over many tasks.
ok1 = df.groupBy("id").applyInPandas(score, schema=df.schema)
ok2 = df.groupby(F.col("id")).applyInPandas(score, schema=df.schema)
ok3 = df.groupBy(["id", "day"]).applyInArrow(score, schema=df.schema)
ok4 = df.groupBy(*cols).applyInPandas(score, schema=df.schema)  # columns via *args
ok5 = df.groupBy(cols).applyInPandas(score, schema=df.schema)  # list variable

# Keyless groupBy with plain aggregates: a global aggregate, Spark does it in parallel.
total = df.groupBy().agg(F.sum("v"))
avg = df.groupBy().avg()
cnt = df.groupBy().count()

# Per-partition pandas: the recommended alternative.
per_part = df.mapInPandas(score, schema=df.schema)

# Bounded input: one small group is fine.
tiny = df.limit(100).groupBy().applyInPandas(score, schema=df.schema)

# Name bound to a keyed grouping.
keyed = df.groupBy("id")
ok6 = keyed.applyInPandas(score, schema=df.schema)

# Name bound in two branches: not proven keyless.
if cols:
    g = df.groupBy("id")
else:
    g = df.groupBy()
ok7 = g.applyInPandas(score, schema=df.schema)

# pandas look-alikes: not Spark.
pdf = pd.DataFrame({"a": [1, 2]})
p1 = pdf.groupby([]).apply(score)
p2 = pdf.groupby("a").apply(score)


class Grouper:
    def groupBy(self):
        return self

    def applyInPandas(self, fn, schema):
        return fn


u = Grouper().groupBy().applyInPandas(score, schema="x")


suppressed = df.groupBy().applyInPandas(score, schema=df.schema)  # noqa: PRF035
