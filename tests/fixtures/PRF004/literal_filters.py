from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

ev = spark.table("events")
win = spark.table("windows")

# An equality against a literal filters rows; it is not a join key.
a = ev.join(win, F.expr("ts BETWEEN start AND end AND kind = 'x'"))  # expect: PRF004
b = ev.join(win, (ev.ts >= win.start) & F.expr("kind = 'x'"))  # expect: PRF004
c = ev.join(win, F.expr('ts > start AND kind = "x"'))  # expect: PRF004
d = ev.join(win, F.expr("ev.id != win.id"))  # expect: PRF004
e = ev.join(win.hint("broadcast"), ev.ts >= win.start)  # expect: PRF004

# Traps: a real key next to a literal filter, or nothing that compares the sides.
k1 = ev.join(win, F.expr("ev.id = win.id AND kind = 'x'"))
k2 = ev.join(win, (ev.ts >= win.start) & F.expr("ev.id = win.id"))
k3 = ev.join(win, F.expr("kind = 'x'"))
k4 = ev.join(win, F.expr("ev.id <=> win.id AND ts > start"))
