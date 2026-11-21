from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
a = spark.table("a")
b = spark.table("b")
flag = spark.conf.get("x") == "1"

# The oracle's columns fact: a name bound on several paths has the columns every path
# agrees on (here: `a`'s, through row-only steps).
if flag:
    m = a.filter("x > 1")
else:
    m = a.filter("x < 0").limit(10)
both_paths = m.union(a)

# A loop that only filters keeps the columns.
w = a
for i in range(3):
    w = w.filter(F.col("x") > i)
loop_filtered = w.union(a)

# Set operations, dropna / fillna / replace and sampleBy keep the receiver's columns.
set_op = a.intersect(b).union(a)
subtracted = a.subtract(b).unionAll(a)
dropped_nulls = a.dropna().union(a.fillna(0))
replaced = a.replace(1, 2).union(a)

# An annotated assignment is followed like a plain one.
z: DataFrame = a.filter("x > 1")
annotated = z.union(a)

# unionByName without allowMissingColumns keeps the receiver's columns.
by_name = a.unionByName(a.filter("x > 1")).union(a)

# These are reported:
# allowMissingColumns=True adds the other side's columns.
missing = a.unionByName(b, allowMissingColumns=True).union(a)  # expect: COR010
# The paths disagree: one side adds a column.
if flag:
    n = a.filter("x > 1")
else:
    n = a.withColumn("y", F.lit(1))
disagree = n.union(a)  # expect: COR010
# A loop that adds a column each pass.
v = a
for i in range(3):
    v = v.withColumn(f"c{i}", F.lit(i))
loop_added = v.union(a)  # expect: COR010
