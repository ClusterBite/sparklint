from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the call as written, arguments included when short.
orders.withColumn("x", F.lit(1))  # expect: COR007 ~'Result of orders.withColumn("x", F.lit(1)) is discarded'
orders.dropDuplicates(["id", "day", "region", "customer"])  # expect: COR007 ~"Result of orders.dropDuplicates(…) is discarded"
