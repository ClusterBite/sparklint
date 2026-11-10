from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf, struct, col
import pandas as pd

spark = SparkSession.builder.getOrCreate()


@udf("string")
def row_key(r):
    return f"{r.id}-{r.name}"


@pandas_udf("double")
def score(s: pd.DataFrame) -> pd.Series:
    return s["a"] * 2


norm = F.udf(lambda *xs: str(xs), "string")

df = spark.table("events")

# struct("*") and its spellings.
a = df.withColumn("k", row_key(F.struct("*")))  # expect: UDF013
b = df.withColumn("k", row_key(struct(col("*"))))  # expect: UDF013
c = df.select(score(F.struct(df["*"])).alias("s"))  # expect: UDF013

# struct over every column of the frame.
d = df.withColumn("k", row_key(F.struct(*df.columns)))  # expect: UDF013
e = df.withColumn("k", row_key(F.struct(df.columns)))  # expect: UDF013
f = df.withColumn("k", row_key(F.struct([F.col(x) for x in df.columns])))  # expect: UDF013
g = df.withColumn("k", row_key(F.struct(*[df[x] for x in df.columns])))  # expect: UDF013

# every column as separate arguments.
h = df.withColumn("k", norm(*df.columns))  # expect: UDF013
i = df.withColumn("k", norm(*(F.col(x).cast("string") for x in df.columns)))  # expect: UDF013

# inline udf value.
j = df.withColumn("k", F.udf(lambda r: str(r), "string")(F.struct("*")))  # expect: UDF013

# filter predicate and typed-param function.
k = df.filter(row_key(F.struct("*")) != "x")  # expect: UDF013


def keyed(frame: DataFrame) -> DataFrame:
    return frame.withColumn("k", row_key(F.struct(*frame.columns)))  # expect: UDF013


# a scalar pandas UDF with an explicit SCALAR function type still counts.
from pyspark.sql.functions import PandasUDFType

scalar = pandas_udf(lambda s: s, "string", functionType=PandasUDFType.SCALAR)
m = df.withColumn("k", scalar(F.struct("*")))  # expect: UDF013
