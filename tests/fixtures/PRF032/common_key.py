from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

orders = spark.table("orders")
cust = spark.table("customers")

# The same key equality ANDed into every OR branch: Spark factors it out of the OR
# ((k & a) | (k & b) -> k & (a | b)), so the join keeps its hash key.
a = orders.join(cust, ((orders.cust_id == cust.id) & (orders.ts > cust.since)) | ((orders.cust_id == cust.id) & cust.since.isNull()))
b = orders.join(cust, ((orders.cust_id == cust.id) & (orders.kind == "a")) | ((cust.id == orders.cust_id) & (orders.kind == "b")))
c = orders.join(
    cust,
    ((orders.k == cust.k) & (orders.x == cust.x))
    | ((orders.k == cust.k) & (orders.y == cust.y))
    | ((orders.k == cust.k) & orders.z.isNull()),
)
d = orders.join(cust, (orders.k.eqNullSafe(cust.k) & (orders.v > 1)) | (orders.k.eqNullSafe(cust.k) & (orders.w > 1)))
cond = ((orders.k == cust.k) & (orders.v > 1)) | ((orders.k == cust.k) & (orders.w > 1))
e = orders.join(cust, cond)

# Same in SQL text.
f = orders.join(cust, F.expr("(o.k = c.k AND o.ts > c.since) OR (c.k = o.k AND c.since IS NULL)"))
g = orders.join(cust, F.expr("o.k = c.k AND o.a = 1 OR o.k = c.k AND o.b = 2"))
