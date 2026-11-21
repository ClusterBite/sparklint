from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("events")
n = len(df.collect())  # expect: ACT009
last = df.collect()[-1]  # expect: ACT009
head = df.orderBy("ts").collect()[:5]  # expect: ACT009
