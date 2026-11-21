from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

orders = spark.table("orders")
cust = spark.table("customers")

# A key in only some OR branches: no key survives the OR.
a = orders.join(cust, ((orders.k == cust.k) & (orders.x > 1)) | ((orders.j == cust.j) & (orders.x > 1)))  # expect: PRF032
b = orders.join(cust, ((orders.k == cust.k) & (orders.x > 1)) | (orders.j == cust.j) | ((orders.k == cust.k) & (orders.y > 1)))  # expect: PRF032

# The shared part is a literal filter, not a key.
c = orders.join(cust, ((orders.flag == 1) & (orders.k == cust.k)) | ((orders.flag == 1) & (orders.j == cust.j)))  # expect: PRF032

# `==` in one branch, eqNullSafe in the other: different expressions.
d = orders.join(cust, ((orders.k == cust.k) & (orders.x > 1)) | orders.k.eqNullSafe(cust.k))  # expect: PRF032

# The shared equality is nested in its own OR, not ANDed into the branch.
e = orders.join(cust, (((orders.k == cust.k) | (orders.m == cust.m)) & (orders.x > 1)) | ((orders.k == cust.k) | (orders.n == cust.n)))  # expect: PRF032

# SQL: key only in some branches, a range, or masked quoted names.
f = orders.join(cust, F.expr("(o.k = c.k AND o.a = 1) OR (o.j = c.j AND o.a = 1)"))  # expect: PRF032
g = orders.join(cust, F.expr("(o.k >= c.k AND o.j = c.j) OR (o.k >= c.k)"))  # expect: PRF032
h = orders.join(cust, F.expr("(`o`.`k` = `c`.`k` AND o.j = c.j) OR (`o`.`m` = `c`.`m`)"))  # expect: PRF032
