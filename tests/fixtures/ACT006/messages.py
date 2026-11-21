from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the count call and the receiver to call isEmpty() on as written.
if orders.filter(orders.amount > 0).count() > 0:  # expect: ACT006 ~"orders.filter(orders.amount > 0).count() used as an emptiness check" ~"use orders.filter(orders.amount > 0).isEmpty()."
    pass
n = orders.count()  # expect: ACT006 ~"orders.count() is only used as an emptiness check — use orders.isEmpty()" help~"Keep count() only when the number itself is needed"
if n == 0:
    pass
e = orders.rdd.isEmpty()  # expect: ACT006 ~"orders.rdd.isEmpty() converts the DataFrame to an RDD first — use orders.isEmpty()." help~"stops at the first row"
