from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

ev = spark.table("events")
win = spark.table("windows")

# F.expr text as the str= keyword or through a name.
a = ev.join(win, F.expr(str="ts BETWEEN start AND end"))  # expect: PRF004
RANGE = "ts >= start AND ts < end"
b = ev.join(win, F.expr(RANGE))  # expect: PRF004

# Not reported: an equality in the text.
KEYED = "ev.id = win.id AND ts > start"
c = ev.join(win, F.expr(KEYED))
