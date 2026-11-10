import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("sales")

COURSES = ["dotNET", "Java"]

# Values given: no extra job.
a = df.groupBy("year").pivot("course", ["dotNET", "Java"]).sum("earnings")
b = df.groupBy("year").pivot("course", COURSES).sum("earnings")
c = df.groupBy("year").pivot("course", values=COURSES).sum("earnings")
d = df.groupBy("year").pivot(pivot_col="course", values=COURSES).count()

# Arguments forwarded: values may be inside.
args = ("course", COURSES)
e = df.groupBy("year").pivot(*args).count()
kw = {"pivot_col": "course", "values": COURSES}
f = df.groupBy("year").pivot(**kw).count()

# pandas look-alikes: DataFrame.pivot(index, columns, values) is not Spark.
pdf = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
p1 = pdf.pivot(index="a", columns="b", values="c")
p2 = pdf.groupby("a").pivot  # attribute only
psdf = ps.DataFrame({"a": [1], "b": [2], "c": [3]})
p3 = psdf.pivot(index="a", columns="b", values="c")


# A user class with a pivot method.
class Table:
    def pivot(self, col):
        return col


t = Table().pivot("x")
