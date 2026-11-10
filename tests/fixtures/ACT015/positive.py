from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

q = df.approxQuantile("latency", [0.25, 0.5, 0.75], 0.0)  # expect: ACT015
q2 = df.approxQuantile("latency", [0.5], 0)  # expect: ACT015
q3 = df.approxQuantile(col="latency", probabilities=[0.5], relativeError=0.0)  # expect: ACT015
q4 = df.stat.approxQuantile("latency", [0.99], 0.0)  # expect: ACT015
q5 = df.approxQuantile(["a", "b"], [0.5], -0.0)  # expect: ACT015

EXACT = 0.0
q6 = df.approxQuantile("latency", [0.5], EXACT)  # expect: ACT015


def bounds(frame: DataFrame, col: str):
    lo, hi = frame.approxQuantile(col, [0.01, 0.99], 0)  # expect: ACT015
    return lo, hi


for c in ["a", "b"]:
    spark.table("t").approxQuantile(c, [0.5], 0.0)  # expect: ACT015
