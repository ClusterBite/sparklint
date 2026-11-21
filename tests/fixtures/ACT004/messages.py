from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.table('t')
# The source the message names matches how the rows were pulled.
rows = df.collect()
for r in rows: pass  # expect: ACT004 ~"(rows from df.collect())"
it = df.toLocalIterator()
for r in it: pass  # expect: ACT004 ~"(rows from df.toLocalIterator())"
for r in df.toLocalIterator(True): pass  # expect: ACT004 ~"(for row in df.toLocalIterator(True))"
for r in df.toPandas().to_dict('records'): pass  # expect: ACT004 ~"(df.toPandas().to_dict('records'))"
for r in df.toArrow().to_pylist(): pass  # expect: ACT004 ~"(df.toArrow().to_pylist())"
if x:
    rows = df.collect()
else:
    rows = df.toLocalIterator()
for r in rows: pass  # expect: ACT004 ~"(rows collected from a DataFrame)"
orders = spark.table("orders")
# Every shape names the real receiver, never a stand-in `df` / `rdd`.
for r in orders.collect(): pass  # expect: ACT004 ~"(for row in orders.collect())"
for r in orders.rdd.collect(): pass  # expect: ACT004 ~"(for row in orders.rdd.collect())"
for r in orders.toArrow().to_pylist(): pass  # expect: ACT004 ~"(orders.toArrow().to_pylist())"
for i, r in orders.toPandas().iterrows(): pass  # expect: ACT004 ~"(orders.toPandas().iterrows())"
pdf = orders.toPandas()
for i, r in pdf.iterrows(): pass  # expect: ACT004 ~"(pdf.iterrows())"
order_rows = orders.collect()
for r in order_rows: pass  # expect: ACT004 ~"(rows from orders.collect())"
