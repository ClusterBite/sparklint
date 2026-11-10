from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

ev = spark.table("events")
win = spark.table("windows")
a = ev.join(win, "id")
b = ev.join(win, ["id", "day"], "left")
c = ev.join(win, (ev.id == win.id) & (ev.ts >= win.start))
d = ev.join(win, ev.id.eqNullSafe(win.id))
e = ev.join(win, F.expr("ev.id = win.id AND ts > start"))
cond = ev.id == win.id
f = ev.join(win, cond)
g = ",".join(["a", "b"])
h = ev.join(win)  # cartesian: PRF003's


def _match(a, b) -> "Column":
    return a == b


j = ev.join(win, _match(F.col("a"), F.col("b")) | F.lit(False))


def joined(left: DataFrame, right: DataFrame, cond):
    return left.join(right, cond)
