from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the receiver as written, a long one hop by hop.
r = orders.rdd  # expect: DBX002 ~"orders.rdd is not supported"
t = spark.read.format("text").load("/data/very/long/path").rdd  # expect: DBX002 ~"spark.read.format(…).load(…).rdd is not supported"
