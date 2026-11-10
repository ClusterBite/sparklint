from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

orders = spark.table("orders")
cust = spark.table("customers")

# Two alternative keys ORed together.
a = orders.join(cust, (orders.cust_id == cust.id) | (orders.alt_id == cust.id))  # expect: PRF032
b = orders.join(cust, on=(orders.email == cust.email) | (orders.phone == cust.phone), how="left")  # expect: PRF032

# Condition held in a variable.
cond = (orders.cust_id == cust.id) | (orders.alt_id == cust.legacy_id)
c = orders.join(cust, cond, "inner")  # expect: PRF032

# OR of an equality and a range test, ANDed with a literal filter: still no usable key.
d = orders.join(cust, ((orders.cust_id == cust.id) | (orders.ts < cust.since)) & (cust.active == True))  # expect: PRF032

# SQL expression string.
e = orders.join(cust, F.expr("orders.cust_id = customers.id OR orders.alt_id = customers.id"))  # expect: PRF032
f = orders.join(cust, F.expr("(o.k1 = c.k1 or o.k2 = c.k2) AND c.region = 'eu'"))  # expect: PRF032

# eqNullSafe inside the OR.
g = orders.join(cust, orders.cust_id.eqNullSafe(cust.id) | orders.alt_id.eqNullSafe(cust.id))  # expect: PRF032

# Conditions in a list are ANDed: the only element is an OR.
h = orders.join(cust, [(orders.cust_id == cust.id) | (orders.alt_id == cust.id)])  # expect: PRF032


def match(left: DataFrame, right: DataFrame) -> DataFrame:
    return left.join(right, (left.k == right.k) | (left.k2 == right.k))  # expect: PRF032


# Small (broadcast) side: still reported, as info.
i = orders.join(F.broadcast(cust), (orders.cust_id == cust.id) | (orders.alt_id == cust.id))  # expect: PRF032


# An opaque helper INSIDE the OR doesn't help: the OR still yields no key.
def _match(a, b):
    return a == b


j = orders.join(cust, _match(orders.k, cust.k) | (orders.k2 == cust.k2))  # expect: PRF032
k = orders.join(cust, F.expr("o.k1 = c.k1 OR o.k2 = c.k2 AND c.flag = true"))  # expect: PRF032
l = orders.join(cust, F.expr("o.x = c.x OR o.y < 1 AND o.k = c.k"))  # expect: PRF032
