from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# selectExpr items unpacked from a list literal; F.expr(str=...); a name.
a = df.selectExpr(*["user", "collect_list(page) over (partition by user order by ts)"])  # expect: PRF031
b = df.withColumn("h", F.expr(str="collect_set(page) over (partition by user order by ts)"))  # expect: PRF031
HIST = "collect_list(page) OVER (PARTITION BY user ORDER BY ts) AS hist"
c = df.selectExpr("user", HIST)  # expect: PRF031
