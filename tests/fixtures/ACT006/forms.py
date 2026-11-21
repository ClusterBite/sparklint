from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.pandas as ps

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")
other = spark.table("other")
n = 5

# Every comparison form, either side, and the test contexts.
if df.count() >= 1:  # expect: ACT006 help~"len(df.take(1)) == 0"
    pass
if 0 == df.count():  # expect: ACT006
    pass
if df.count():  # expect: ACT006
    pass
label = "rows" if df.count() else "empty"  # expect: ACT006
assert df.count() > 0, "no rows"  # expect: ACT006
has_rows = df.count() != 0  # expect: ACT006
is_empty = df.count() < 1  # expect: ACT006
none_left = df.count() <= 0  # expect: ACT006
if df.count() == 0 or other.count() == 0:  # expect: ACT006, ACT006
    pass


def has_data(rows_df):
    return rows_df.count() > 0  # expect: ACT006


# Not emptiness checks, or cheap to count.
more_than_one = df.count() > 1
if spark.sql(f"SELECT * FROM t LIMIT {n}").count() > 0:
    pass
if df.agg(F.max("x")).count() > 0:
    pass
if len(df.take(1)) > 0:
    pass
psdf = ps.read_parquet("/x")
if psdf.count() > 0:
    pass
total = other.count()
if total > 0 and total < 10:
    pass
# Not followed: a count wrapped in a call, an untyped parameter.
if bool(df.count()):
    pass


def any_rows(x):
    return x.count() > 0
