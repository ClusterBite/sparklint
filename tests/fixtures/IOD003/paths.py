from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
HINT = "coalesce"

# A frame bound on two paths that agree: the steps below the write are followed.
if spark.conf.get("mode") == "x":
    one = df.coalesce(1)  # expect: IOD003
else:
    one = df.coalesce(1)
one.write.parquet("/p")

# A hint name held in a constant.
df.hint(HINT, 1).write.parquet("/q")  # expect: IOD003

# Paths that disagree: the walk stops at the name.
if spark.conf.get("mode") == "x":
    mixed = df.coalesce(1)
else:
    mixed = df
mixed.write.parquet("/r")

# Every path ends in one partition, through different steps: reported at the first
# path's step.
if spark.conf.get("mode") == "x":
    single = df.coalesce(1)  # expect: IOD003 ~"coalesce(1) before"
else:
    single = df.repartition(1)
single.write.parquet("/s")

# One path adds a narrow step after the same one-partition step.
if spark.conf.get("mode") == "x":
    narrowed = df.coalesce(1)  # expect: IOD003
else:
    narrowed = df.coalesce(1).select("x")
narrowed.write.parquet("/t")

# One path shuffles after the step: that path writes in parallel.
if spark.conf.get("mode") == "x":
    regrouped = df.coalesce(1)
else:
    regrouped = df.coalesce(1).groupBy("k").count()
regrouped.write.parquet("/u")

# Small on one path only: still a warning.
if spark.conf.get("mode") == "x":
    part = df.limit(10).coalesce(1)  # expect: IOD003[warning]
else:
    part = df.coalesce(1)
part.write.parquet("/v")

# Small on every path: info.
if spark.conf.get("mode") == "x":
    tiny = df.limit(10).coalesce(1)  # expect: IOD003[info]
else:
    tiny = df.limit(5).repartition(1)
tiny.write.parquet("/w")
