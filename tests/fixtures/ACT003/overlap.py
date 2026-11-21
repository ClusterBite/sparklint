# Overlap fixture: linted alone, this rule fires on every marked line; with ALL or the
# default preset a higher-precedence rule fires at the same site and this finding is
# dropped (sparklint_linter::overlap::PRECEDENCE; checked by the preset pass).
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")

for _, r in df.toPandas().iterrows():  # expect: ACT003
    print(r)

# ACT009 (driver compute over the pull) wins at the same toPandas() call.
n = len(df.toPandas())  # expect: ACT003
total = sum(df.toPandas()["x"])  # expect: ACT003
pdf = df.toPandas()  # expect: ACT003
top = max(pdf["x"])
