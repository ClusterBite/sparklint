from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("events")

# A plain column beside the aggregates: one row per input row.
a = df.select([F.max("b"), "c"]).collect()[0]  # expect: ACT009

# A window aggregate is one value per row.
b = df.selectExpr("max(v) over (partition by k) as m").collect()[0]  # expect: ACT009

# A non-aggregate expression next to an aggregate one.
c = df.selectExpr("max(v)", "k").collect()[0]  # expect: ACT009

# Grouping keys: one row per group.
d = df.groupBy("k").agg(F.max("v")).collect()[0]  # expect: ACT009

# A keyless grouped pandas function returns any number of rows.
e = df.groupBy().applyInPandas(lambda p: p, "v long").collect()[0]  # expect: ACT009
