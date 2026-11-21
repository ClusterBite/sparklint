from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the call as written, RDD steps included.
ids = orders.rdd.map(lambda r: r.id).collect()  # expect: ACT011 ~"orders.rdd.map(lambda r: r.id).collect() runs the RDD steps in Python workers" help~"mapInPandas"
few = orders.rdd.map(lambda r: r.id).take(5)  # expect: ACT011 ~"orders.rdd.map(lambda r: r.id).take(5) runs the RDD steps" help~"Spark Connect"
# The direct form names the DataFrame method that returns the same rows.
rows = orders.rdd.take(5)  # expect: ACT011[warning] ~"converts the DataFrame to an RDD only to read rows back; `orders.take(5)` returns the same rows" !~"Python workers" help~"standard access mode"
