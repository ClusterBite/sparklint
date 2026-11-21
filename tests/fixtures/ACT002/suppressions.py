from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")

# Suppressed: comments name rules with the configuration's grammar (code, rule name,
# cylint code, tag, ALL); other linters' codes next to it are skipped.
a = df.collect()  # noqa: ACT002
b = df.collect()  # noqa: collect-unbounded
c = df.collect()  # noqa: CY001
d = df.collect()  # noqa: ALL
e = df.collect()  # sparklint: ignore[tag:cost]
f = df.collect()  # noqa: E501, ACT002
g = df.collect()  # noqa: ACT002 - small lookup table
h = df.collect()  # noqa

# Not suppressed: a token that names no rule suppresses nothing (it used to silence
# every rule on the line); the CLI warns about it.
i = df.collect()  # noqa: collect-without-limit  # expect: ACT002
j = df.collect()  # sparklint: ignore[bogus]  # expect: ACT002
k = df.collect()  # noqa: CY999  # expect: ACT002
m = df.collect()  # noqa: E501  # expect: ACT002
n = df.collect()  # noqa: PRF001  # expect: ACT002
