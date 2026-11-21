import logging

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger(__name__)
orders = spark.table("orders")

# Every result-returning pull counts, the same list as everywhere else.
orders.tail(5)  # expect: ACT007 ~"orders.tail(5) result is discarded"
orders.toArrow()  # expect: ACT007
orders.toLocalIterator()  # expect: ACT007
logger.info("last rows: %s", orders.tail(3))  # expect: ACT007 ~"is computed only to be logged"
print(orders.toArrow().num_rows)  # expect: ACT007

# Traps: side-effect actions, a driver-sized frame, a used result.
orders.foreach(print)
orders.limit(5).toArrow()
rows = orders.tail(5)
