# Databricks notebook source
orders = spark.table("orders")
events = spark.readStream.table("events")

# COMMAND ----------

# The message names the call as written.
orders.show(20, truncate=False)  # expect: ACT010 ~"orders.show(20, truncate=False) left in pipeline code" help~"noqa: ACT010"
display(orders)  # expect: ACT010 ~"display(orders) left in pipeline code"
# A long argument: the frame is named hop by hop, the rest elided.
display(orders.filter("amount > 100 and region = 'EU'"))  # expect: ACT010 ~"display(orders.filter(…)) left"
display(orders.groupBy("region").count(), streamName="x")  # expect: ACT010 ~'display(orders.groupBy("region").count(), …) left'
# printSchema / explain run no job: their own wording.
orders.printSchema()  # expect: ACT010[info] ~"orders.printSchema() left in pipeline code prints to the driver log: it runs no job" !~"triggers extra jobs"
orders.explain("formatted")  # expect: ACT010[info] ~"runs no job"
# Debug streaming sinks: their own wording and help.
q = events.writeStream.format("console").start()  # expect: ACT010 ~'events.writeStream.format("console") is a debug sink' help~"Delta table"
m = events.writeStream.format("memory").queryName("m").start()  # expect: ACT010 ~"kept in driver memory" !~"triggers extra jobs"
