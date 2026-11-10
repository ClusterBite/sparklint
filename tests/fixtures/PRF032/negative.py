from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

orders = spark.table("orders")
cust = spark.table("customers")

# Plain equi-joins.
a = orders.join(cust, "id")
b = orders.join(cust, ["id", "day"], "left")
c = orders.join(cust, orders.cust_id == cust.id)

# An equality ANDed next to the OR still gives Spark a hash key.
d = orders.join(cust, (orders.cust_id == cust.id) & ((orders.kind == cust.kind) | cust.kind.isNull()))
e = orders.join(cust, [orders.cust_id == cust.id, (orders.a == cust.a) | (orders.b == cust.b)])
f = orders.join(cust, F.expr("o.id = c.id AND (o.a = c.a OR o.b IS NULL)"))
key = orders.cust_id == cust.id
g = orders.join(cust, key & ((orders.a == cust.a) | (orders.b == cust.b)))

# OR without any equality: PRF004's (non-equi join), not this rule.
h = orders.join(cust, (orders.ts < cust.start) | (orders.ts > cust.end))
i = orders.join(cust, F.expr("o.ts < c.start OR o.ts > c.end"))

# OR inside a string literal value, or a column named "or".
j = orders.join(cust, F.expr("o.label = 'a or b'"))
k = orders.join(cust, F.expr("o.`or` = c.`or`"))

# Opaque conjunct next to the OR: it may hold the equi-key.
def _match(a, b):
    return a == b


m = orders.join(cust, _match(orders.k, cust.k) & ((orders.a == cust.a) | (orders.b == cust.b)))
other_cond = build_condition()
n = orders.join(cust, [other_cond, (orders.k2 == cust.k2) | (orders.k3 == cust.k3)])
neg = ~((orders.a == cust.a) | (orders.b == cust.b))

# Unknown receivers and plain Python.
def joined(left, right):
    return left.join(right, (left.k == right.k) | (left.k2 == right.k))


s = ",".join(["a", "b"])
mask = (1 == 1) | (2 == 2)

# BETWEEN ... AND inside SQL with an equality conjunct.
p = orders.join(cust, F.expr("o.id = c.id AND o.ts BETWEEN c.s AND c.e"))

q = orders.join(cust, (orders.cust_id == cust.id) | (orders.alt_id == cust.id))  # noqa: PRF032
r = orders.join(cust, F.expr("(o.id = c.id) AND (o.a = c.a OR o.b = c.b)"))
t = orders.join(cust, F.expr("o.flag = 1 OR o.kind = 'x'"))
