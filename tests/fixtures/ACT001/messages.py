from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# The message names the call as written: receiver, method and (short) arguments.
n = orders.count()  # expect: ACT001 ~"Spark action: orders.count() triggers a job (kind: job)."
top = orders.take(5)  # expect: ACT001 ~"Spark action: orders.take(5) triggers"
rows = orders.filter("amount > 100 and region = 'EU'").collect()  # expect: ACT001 ~"orders.filter(…).collect()" !~"df."
orders.write.mode("append").saveAsTable("out")  # expect: ACT001 ~'orders.write.mode("append").saveAsTable("out")'
orders.write.format("delta").option("mergeSchema", "true").saveAsTable("t")  # expect: ACT001 ~'orders.write.format(…).option(…).saveAsTable("t")'
