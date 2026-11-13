from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

import pandas as pd

df = spark.read.parquet("/data")
x = None
if x == None:
    pass
pdf = pd.DataFrame({"a": [1]})
y = pdf["a"] == None
a = df.filter(F.col("a").isNull())
from pyspark.sql import Column


def pick(c: Column | None = None) -> Column:
    return F.lit(0) if c is None else c


def keep(cols: list) -> list:
    lines = [F.col(x) for x in cols]
    return [ln for ln in lines if ln is not None]


# A helper's Column annotation is a hint; `helper(..) is None` is the Optional check.
def segment_filter(values: dict | None) -> Column | None:
    if not values:
        return None
    return F.col("seg") == values["seg"]


def safe_expr(text: str) -> Column:
    return F.expr(text)


assert segment_filter(None) is None
assert safe_expr("a > 1") is not None
