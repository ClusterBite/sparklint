from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the collect call as written.
first = orders.filter(orders.amount > 100).collect()[0]  # expect: ACT009 ~"orders.filter(orders.amount > 100).collect()[0] pulls every row"
total = len(orders.where("amount > 0").collect())  # expect: ACT009 ~'len() computed on the driver from orders.where("amount > 0").collect()'
