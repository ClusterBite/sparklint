from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
# A loop body that also runs a Spark action per row gets the N+1 wording.
for row in orders.collect():  # expect: ACT004 ~"(for row in orders.collect())" ~"The loop body also runs a Spark action per row (N+1 jobs)."
    spark.table("items").filter(f"order_id = {row.id}").count()
for row in orders.collect():  # expect: ACT004 !~"N+1"
    print(row.id)
