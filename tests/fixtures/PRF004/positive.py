from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

ev = spark.table("events")
win = spark.table("windows")
j = ev.join(win, (ev.ts >= win.start) & (ev.ts < win.end))  # expect: PRF004
j2 = ev.join(win, ev.ts.between(win.start, win.end), "inner")  # expect: PRF004
j3 = ev.join(win, F.expr("ts BETWEEN start AND end"))  # expect: PRF004
cond = (ev.ts > win.start) & (ev.kind == "x")
j4 = ev.join(win, cond, "left")  # expect: PRF004
