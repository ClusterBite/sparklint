from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.table("orders")
target = DeltaTable.forName(spark, "sales.orders")

# A write / stream / merge chain that stops before the call that runs it does nothing.
orders.write.mode("overwrite").format("delta")  # expect: COR007[error] ~'orders.write.mode("overwrite").format("delta") sets up a write that never runs' ~"save() / saveAsTable() / insertInto()" help~"Finish the chain"
orders.write.partitionBy("day").option("mergeSchema", "true")  # expect: COR007 ~"a write that never runs"
orders.writeStream.format("delta").option("checkpointLocation", "/chk")  # expect: COR007 ~"sets up a streaming query that never runs" ~"start() / toTable()"
orders.writeTo("sales.orders").using("delta")  # expect: COR007 ~"append() / overwrite()"
orders.mergeInto("sales.orders", F.col("id") == F.col("t.id")).whenMatched().updateAll()  # expect: COR007 ~"a merge that never runs" ~"until merge() is called"
target.alias("t").merge(orders.alias("s"), "t.id = s.id").whenMatchedUpdateAll()  # expect: COR007 ~"a merge that never runs" ~"until execute() is called"
target.optimize().where("day = '2026-01-01'")  # expect: COR007 ~"sets up an OPTIMIZE that never runs" ~"executeCompaction() / executeZOrderBy()"
DeltaTable.create(spark).tableName("sales.daily").addColumn("id", "INT")  # expect: COR007 ~"sets up a table creation that never runs"
orders.write  # expect: COR007 ~"orders.write sets up a write"


def save(df: DataFrame):
    df.write.format("parquet").mode("append")  # expect: COR007


# Lazy checkpoints only mark the frame they return.
orders.checkpoint(eager=False)  # expect: COR007 ~"Result of orders.checkpoint(eager=False) is discarded"
orders.localCheckpoint(False)  # expect: COR007
# observe() returns the observed frame; discarded, its metrics are never filled in.
orders.observe("metrics", F.count(F.lit(1)))  # expect: COR007
# DataFrameStatFunctions transformations return a new frame too.
orders.stat.sampleBy("region", {"eu": 0.1})  # expect: COR007

# Not reported: chains that run, writers held in a name (they change in place),
# eager checkpoints, stat calls that return values.
orders.write.mode("overwrite").saveAsTable("t")
orders.writeStream.format("delta").option("checkpointLocation", "/chk").start()
target.merge(orders, "t.id = s.id").whenMatchedUpdateAll().execute()
target.optimize().executeZOrderBy("id")
DeltaTable.create(spark).tableName("sales.daily").execute()
writer = orders.write.mode("overwrite")
writer.format("delta")
writer.option("overwriteSchema", "true")
writer.saveAsTable("t")
stream = orders.writeStream
stream.format("delta")
stream.start()
optimizer = target.optimize()
optimizer.where("day = '2026-01-01'")
optimizer.executeCompaction()
builder = DeltaTable.create(spark)
builder.tableName("sales.daily")
builder.execute()
orders.checkpoint()
orders.checkpoint(eager=True)
orders.localCheckpoint()
orders.stat.corr("a", "b")
target.toDF()
try:
    orders.write.format("bogus")
except Exception:
    pass
