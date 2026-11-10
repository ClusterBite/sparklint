import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list
from pyspark.sql.window import Window as W

# default frame of an ordered window: unbounded preceding .. current row
w = Window.partitionBy("user").orderBy("ts")
events = F.collect_list("page").over(w)  # expect: PRF031

# explicit growing frame
running = Window.partitionBy("user").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
pages = F.collect_list("page").over(running)  # expect: PRF031

# pyspark maps -sys.maxsize to unbounded preceding
r2 = W.partitionBy("k").orderBy("ts").rangeBetween(-sys.maxsize, 0)
s = F.collect_set("v").over(r2)  # expect: PRF031

# keyword bounds, frame set before orderBy
r3 = W.partitionBy("k").rowsBetween(start=Window.unboundedPreceding, end=0).orderBy("ts")
a = F.array_agg("v").over(r3)  # expect: PRF031

# directly imported function, inline window
b = collect_list("v").over(W.partitionBy("k").orderBy("ts"))  # expect: PRF031

# no partitionBy: also one partition (reported once, under this rule)
c = F.collect_list("v").over(Window.orderBy("ts"))  # expect: PRF031


def history(df: DataFrame) -> DataFrame:
    hw = Window.partitionBy("id").orderBy(F.col("ts").desc())
    return df.withColumn("hist", F.collect_list(F.struct("ts", "v")).over(hw))  # expect: PRF031


# a later frame replaces an earlier bounded one
r4 = W.partitionBy("k").orderBy("ts").rowsBetween(-3, 0).rowsBetween(Window.unboundedPreceding, -1)
d = F.collect_list("v").over(r4)  # expect: PRF031
