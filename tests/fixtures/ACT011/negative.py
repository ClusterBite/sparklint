from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.table("events")
rows = df.collect()
rdd = spark.sparkContext.parallelize([1, 2, 3])
vals = rdd.collect()


class Holder:
    rdd = None


h = Holder()
x = h.rdd.collect()
