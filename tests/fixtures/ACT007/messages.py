from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the call as written, arguments included.
orders.filter("amount > 1").collect()  # expect: ACT007 ~'orders.filter("amount > 1").collect() result is discarded'
print(orders.head(3))  # expect: ACT007 ~"orders.head(3) is computed only to be logged"
