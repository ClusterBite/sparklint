from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, struct
import pandas as pd

spark = SparkSession.builder.getOrCreate()


@udf("string")
def row_key(r):
    return f"{r.id}-{r.name}"


norm = F.udf(lambda *xs: str(xs), "string")

df = spark.table("events")
pdf = pd.DataFrame({"a": [1]})
wanted = ["id", "name"]

# only the columns the function reads.
a = df.withColumn("k", row_key(F.struct("id", "name")))
b = df.withColumn("k", norm(F.col("id"), F.col("name")))
c = df.withColumn("k", norm(*wanted))
d = df.withColumn("k", row_key(F.struct(*wanted)))

# filtered comprehension: a chosen subset.
e = df.withColumn("k", norm(*[x for x in df.columns if x.startswith("feat_")]))

# built-ins over the whole row are fine.
f = df.withColumn("j", F.to_json(F.struct("*")))
g = df.withColumn("h", F.xxhash64(*df.columns))
h = df.withColumn("s", F.struct(*df.columns))

# not a UDF.


def helper(x):
    return x


i = df.withColumn("k", helper(F.struct("*")))

# pandas frame columns are not Spark columns.
j = norm(*pdf.columns)

# one column named via struct with "*" inside a string literal.
k = df.withColumn("k", row_key(F.struct("a*b")))

# a struct("*") nested inside another built-in, not the UDF argument itself.
m = df.withColumn("k", row_key(F.to_json(F.struct("*"))))

# grouped-map pandas UDF built with a function type takes the whole frame by design.
from pyspark.sql.functions import pandas_udf, PandasUDFType


def flat(pdf):
    return pdf


grouped = pandas_udf(flat, "a long", functionType=PandasUDFType.GROUPED_MAP)
o = grouped(*[df[x] for x in df.columns])
eval_type = 201
grouped2 = pandas_udf(flat, returnType="a long", functionType=eval_type)
q = grouped2(*[df[x] for x in df.columns])
if eval_type:
    grouped3 = pandas_udf(flat, "a long", functionType=eval_type)
else:
    grouped3 = pandas_udf(flat, "b long", functionType=eval_type)
r = grouped3(*[df[x] for x in df.columns])

n = df.withColumn("k", row_key(F.struct("*")))  # noqa: UDF013
