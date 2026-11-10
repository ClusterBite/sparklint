from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("events")
n = len(df.collect())  # expect: ACT009
mx = max(r.ts for r in df.collect())  # expect: ACT009
total = sum([r.amount for r in df.select("amount").collect()])  # expect: ACT009
first = df.collect()[0]  # expect: ACT009
v = df.orderBy("ts").collect()[0][0]  # expect: ACT009
rows = df.collect()
distinct_users = set(rows)  # expect: ACT009
