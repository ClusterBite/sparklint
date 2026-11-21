from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("k").orderBy("ts")

# A frame reaching back a million rows or more starts at the partition start in practice.
a = F.collect_list("v").over(w.rowsBetween(-(2**63), 0))  # expect: PRF031
b = F.collect_list("v").over(w.rowsBetween(-10**9, Window.currentRow))  # expect: PRF031
c = F.collect_set("v").over(w.rowsBetween(-1_000_000, -1))  # expect: PRF031

# Bounded frames below the threshold, or an end that is unbounded too.
d = F.collect_list("v").over(w.rowsBetween(-999_999, 0))
e = F.collect_list("v").over(w.rowsBetween(-100, 0))
f = F.collect_list("v").over(w.rowsBetween(-(2**63), 10**9))
