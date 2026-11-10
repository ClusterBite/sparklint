import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# all values per key: the recommended form
def per_key(df: DataFrame) -> DataFrame:
    return df.groupBy("k").agg(F.sort_array(F.collect_list(F.struct("ts", "v"))).alias("vs"))


# bounded frame: last 3 values
last3 = Window.partitionBy("k").orderBy("ts").rowsBetween(-3, Window.currentRow)
x = F.collect_list("v").over(last3)

# whole-partition frame (unordered window or explicit unbounded..unbounded)
x = F.collect_list("v").over(Window.partitionBy("k"))
full = Window.partitionBy("k").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
x = F.collect_set("v").over(full)
full2 = Window.partitionBy("k").orderBy("ts").rangeBetween(-sys.maxsize, sys.maxsize)
x = F.collect_set("v").over(full2)

# forward-looking frame: current row .. end
fwd = Window.partitionBy("k").orderBy("ts").rowsBetween(Window.currentRow, Window.unboundedFollowing)
x = F.collect_list("v").over(fwd)

# a later bounded frame replaces the growing one
w2 = Window.partitionBy("k").orderBy("ts").rowsBetween(Window.unboundedPreceding, 0).rowsBetween(-5, 0)
x = F.collect_list("v").over(w2)

# unknown bounds (variables): not reported
lo, hi = -10, 0
wv = Window.partitionBy("k").orderBy("ts").rowsBetween(lo, hi)
x = F.collect_list("v").over(wv)

# other aggregates over a running window are linear
run = Window.partitionBy("k").orderBy("ts")
x = F.sum("v").over(run)
x = F.last("v", ignorenulls=True).over(run)
x = F.size(F.collect_list("v")).alias("n")


# user function with the same name
def collect_list(c):
    return c


class Spec:
    def over(self, w):
        return w


y = Spec().over(run)
z = collect_list("v").over(run)

# unknown window object
def f(win):
    return F.collect_list("v").over(win)


x = F.collect_list("v").over(run)  # noqa: PRF031
