from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

updates = spark.read.parquet("/updates")
target = DeltaTable.forName(spark, "sales")

# delta-spark table operations run when called.
target.delete("ts < '2020-01-01'")  # expect: ACT001 ~"target.delete(" ~"(kind: write)"
target.update(condition="id = 1", set={"x": "1"})  # expect: ACT001
target.updateExpr("id = 1", {"x": "1"})  # expect: ACT001
target.vacuum(168)  # expect: ACT001
target.restoreToVersion(3)  # expect: ACT001
DeltaTable.forPath(spark, "/data/t").alias("t").generate("symlink_format_manifest")  # expect: ACT001
DeltaTable.convertToDelta(spark, "parquet.`/data/raw`")  # expect: ACT001

# MERGE runs at execute(), after the when-clauses.
(
    target.alias("t")
    .merge(updates.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()  # expect: ACT001 ~"(kind: write)"
)
target.merge(updates, "sales.id = updates.id").whenMatchedDelete().execute()  # expect: ACT001

# A builder held in a name is followed.
pending = target.merge(updates, "sales.id = updates.id").withSchemaEvolution()
pending.whenNotMatchedInsertAll().execute()  # expect: ACT001

# OPTIMIZE runs at executeCompaction() / executeZOrderBy().
target.optimize().where("day = '2024-01-01'").executeCompaction()  # expect: ACT001
target.optimize().executeZOrderBy("id")  # expect: ACT001

# Creating a table through the builder.
DeltaTable.createIfNotExists(spark).tableName("events").addColumn("id", "BIGINT").execute()  # expect: ACT001

# Traps: builders that are never executed, lazy reads, and other `execute()` calls.
builder = target.merge(updates, "sales.id = updates.id").whenMatchedUpdateAll()
history = target.history(1)
frame = target.toDF()
opt = target.optimize().where("day = '2024-01-01'")


class Job:
    def execute(self):
        return 1


Job().execute()
cursor = object()
cursor.execute("DELETE FROM t")
items = {"a": 1}
items.update({"b": 2})
