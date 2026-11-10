# Overlap fixture: linted alone, this rule fires on every marked line; with ALL or the
# default preset a higher-precedence rule fires at the same site and this finding is
# dropped (sparklint_linter::overlap::PRECEDENCE; checked by the preset pass).
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")

for c in ["a", "b", "c", "d"]:
    df = df.withColumn(c, F.lit(1)).drop("x")  # expect: PRF002
