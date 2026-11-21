import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
bad = spark.table("bad_ids")
df = spark.table("events")
ids = [r.id for r in bad.collect()]
flag = os.environ.get("FLAG")

# Reported: SQL text read through a name, the str= keyword, selectExpr items.
cond = f"id IN ({','.join(map(str, ids))})"
a1 = df.filter(cond)  # expect: ACT008 ~"into filter() SQL text"
a2 = df.select(F.expr(str=f"id IN ({','.join(map(str, ids))}) AS hit"))  # expect: ACT008 ~"into F.expr() SQL text"
a3 = df.selectExpr("id", f"id IN ({','.join(map(str, ids))}) AS hit")  # expect: ACT008 ~"into selectExpr() SQL text"
query = f"SELECT * FROM t WHERE id IN ({','.join(map(str, ids))})"
a4 = spark.sql(query)  # expect: ACT008 ~"into spark.sql()"
if flag:
    c5 = f"id IN ({tuple(ids)})"
else:
    c5 = f"id IN ({tuple(ids)})"
a5 = df.where(c5)  # expect: ACT008 ~"into where() SQL text"

# Not reported: an EXPLAIN only plans the query; bindings that disagree.
b1 = spark.sql(f"EXPLAIN SELECT * FROM t WHERE id IN ({tuple(ids)})")
if flag:
    c6 = f"id IN ({tuple(ids)})"
else:
    c6 = "id > 0"
b2 = df.filter(c6)
