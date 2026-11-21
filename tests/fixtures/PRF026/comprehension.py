from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# One distinct aggregate per literal column name: three sets.
a = df.agg(*[F.countDistinct(c) for c in ["user_id", "session_id", "device_id"]])  # expect: PRF026

# A generator, an alias and a wrapped column.
b = df.groupBy("day").agg(  # expect: PRF026
    *(F.countDistinct(F.col(c)).alias(f"n_{c}") for c in ("a", "b", "c"))
)

# Names bound once: the expression list and the column list.
COLS = ["a", "b"]
exprs = [F.count_distinct(c) for c in COLS]
c = df.groupBy("k").agg(F.countDistinct("z"), *exprs)  # expect: PRF026

# select() with the same pattern.
d = df.select(*[F.countDistinct(F.lower(c)) for c in ["a", "b", "c"]])  # expect: PRF026
