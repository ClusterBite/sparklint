import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# Non-zero or non-constant relative error.
a = df.approxQuantile("latency", [0.5], 0.01)
b = df.approxQuantile("latency", [0.5], 1e-3)
tol = float(input())
c = df.approxQuantile("latency", [0.5], tol)
opts = {"relativeError": 0.0}
d = df.approxQuantile("latency", [0.5], **opts)

# pandas / numpy look-alikes.
pdf = pd.DataFrame({"x": [1, 2, 3]})
e = pdf["x"].quantile(0.5)
f = np.quantile(np.array([1, 2]), 0.5)


# A user class with the same method name.
class Stats:
    def approxQuantile(self, col, probs, err):
        return [0.0]


g = Stats().approxQuantile("x", [0.5], 0.0)


# Untyped parameter: the oracle cannot prove a Spark DataFrame.
def untyped(frame):
    return frame.approxQuantile("x", [0.5], 0.0)


# Py4J handle.
class Wrapper:
    def __init__(self, jdf):
        self._jdf = jdf

    def q(self):
        return self._jdf.stat().approxQuantile("x", [0.5], 0.0)
