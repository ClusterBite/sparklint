# Overlap fixture: linted alone, this rule fires on every marked line; with ALL or the
# default preset a higher-precedence rule fires at the same site and this finding is
# dropped (sparklint_linter::overlap::PRECEDENCE; checked by the preset pass).
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
raw = spark.read.table("bronze.events")

# JSON schema inferred over an RDD of strings: IOD016 reports it
schema = spark.read.json(raw.rdd.map(lambda r: r.payload)).schema  # expect: IOD001
