from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# cheap Delta scan + filter: recomputation is cheaper than caching
t = spark.table("events").filter(F.col("x") > 1)
t.write.saveAsTable("a")
t.count()

# cached
agg = spark.table("a").join(spark.table("b"), "k").groupBy("x").count().cache()
agg.write.saveAsTable("o")
agg.count()

# alternative branches: one action per path
agg2 = spark.table("a").groupBy("x").count()
if flag:
    agg2.write.saveAsTable("o2")
else:
    agg2.collect()

# one action
agg3 = spark.table("a").distinct()
agg3.write.saveAsTable("o3")

# derived frame carries both actions: reported there, not on the parent
base = spark.table("a").groupBy("k").count()
small = base.filter(F.col("count") > 1).cache()
small.count()
small.collect()
