from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

orders = spark.table("orders")
cust = spark.table("customers")

# F.expr text as the str= keyword or through a name.
a = orders.join(cust, F.expr(str="o.cust_id = c.id OR o.alt_id = c.id"))  # expect: PRF032
EITHER = "o.cust_id = c.id OR o.alt_id = c.id"
b = orders.join(cust, F.expr(EITHER))  # expect: PRF032
