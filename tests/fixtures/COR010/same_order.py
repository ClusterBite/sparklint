from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
cols = ["id", "name", "ts"]
other_cols = ["ts", "id", "name"]

a = spark.table("a")
b = spark.table("b")

# Both sides re-selected with the same column list (a name, `*name`, or literals):
# the columns are in the same order by construction.
old = a.select(cols)
new = b.select(cols)
same_list = old.union(new)
same_list_chain = old.union(new).union(b.select(cols))
same_star = a.select(*other_cols).union(b.select(*other_cols))
same_literals = a.select("x", "y").union(b.select("x", "y"))
same_literal_list = a.select(["x", "y"]).unionAll(b.select(["x", "y"]))

# The same frame on both sides after row-only steps (filters, limits, sorts, caches).
split = a.filter(F.col("x") > 1).union(a.filter(F.col("x") < 0))
hi = a.where("x > 1")
lo = a.where("x < 0").limit(5).cache()
split_names = hi.union(lo)

# The other side re-selected with the receiver's columns, through a name.
aligned = b.select(a.columns)
aligned_name = a.union(aligned)

# A semi / anti join keeps exactly the left side's columns.
kept = old.join(b, on="id", how="left_anti")
anti = kept.union(new)
semi = old.join(b, "id", "semi").union(new)

# These are reported:
different_lists = a.select(cols).union(b.select(other_cols))  # expect: COR010[warning] help~"select the same column list"
different_literals = a.select("x", "y").union(b.select("y", "x"))  # expect: COR010
added_column = a.withColumn("z", F.lit(1)).union(a)  # expect: COR010
inner = old.join(b, on="id", how="inner").union(new)  # expect: COR010

# The list name was rebound between the two selects.
x = a.select(cols)
cols = ["name", "id", "ts"]
y = b.select(cols)
rebound_list = x.union(y)  # expect: COR010

# The frame name was rebound between the two filters.
f1 = a.filter("x > 1")
a = spark.table("c")
f2 = a.filter("x < 0")
rebound_frame = f1.union(f2)  # expect: COR010

# Tuple unpacking is not followed.
u, v = a.select(cols), b.select(cols)
unpacked = u.union(v)  # expect: COR010
