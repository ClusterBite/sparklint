from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("events")
mx = df.agg(F.max("ts")).collect()[0][0]
top = df.limit(1).collect()[0]
n = df.count()
items = [3, 1, 2]
print(len(items), max(items), sorted(items)[0])
rows = df.collect()
for r in rows:
    print(r)
print(len(rows))

latest = spark.sql("SELECT max(usage_date) FROM cat.sch.t").collect()[0][0]
detail = spark.sql("DESCRIBE DETAIL cat.sch.t").collect()[0]
mx2 = df.select(F.max("ts").alias("m")).collect()[0][0]
