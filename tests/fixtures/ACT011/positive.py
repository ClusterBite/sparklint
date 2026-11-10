from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("events")
rows = df.rdd.collect()  # expect: ACT011
ids = df.rdd.map(lambda r: r.id).collect()  # expect: ACT011
few = df.rdd.take(5)  # expect: ACT011
