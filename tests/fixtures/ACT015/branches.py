from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# A constant chosen on two branches: every binding is 0, so the error is 0.
if spark.conf.get("mode") == "exact":
    ERR = 0.0
else:
    ERR = 0
q = df.approxQuantile("latency", [0.5], ERR)  # expect: ACT015

# Branches that disagree are not a constant.
if spark.conf.get("mode") == "exact":
    TOL = 0.0
else:
    TOL = 0.01
r = df.approxQuantile("latency", [0.5], TOL)
