from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the call as written.
for d in days:
    orders.filter(orders.day == d).count()  # expect: ACT005 ~"Spark action orders.filter(orders.day == d).count() inside a loop"
    orders.filter(orders.day == d).write.mode("append").saveAsTable("t")  # expect: ACT005 ~'write orders.filter(…).write.mode(…).saveAsTable("t") inside a loop'
