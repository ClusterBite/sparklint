from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# A Delta table operation is a job: one per iteration.
def vacuum_all(tables):
    for t in tables:
        DeltaTable.forName(spark, t).vacuum()  # expect: ACT005[info] ~"DeltaTable.forName(spark, t).vacuum()"


def purge_days(days):
    target = DeltaTable.forName(spark, "cat.db.events")
    for d in days:
        target.delete(F.col("day") == d)  # expect: ACT005[warning]


def merge_days(updates, days):
    target = DeltaTable.forName(spark, "cat.db.target")
    for d in days:
        (
            target.alias("t")
            .merge(updates.filter(F.col("day") == d).alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()  # expect: ACT005
        )


def merge_named(sources):
    target = DeltaTable.forName(spark, "cat.db.target")
    for src in sources:
        pending = target.merge(spark.table(src), "target.id = s.id").whenMatchedUpdateAll()
        pending.execute()  # expect: ACT005[info]


def optimize_all(tables):
    for t in tables:
        DeltaTable.forName(spark, t).optimize().executeZOrderBy("id")  # expect: ACT005[info]


# Traps: a builder never executed in the loop, one maintenance call after it.
def build_only(updates, days):
    target = DeltaTable.forName(spark, "cat.db.target")
    for d in days:
        builder = target.merge(updates, "t.id = s.id").whenMatchedUpdateAll()
    target.vacuum()
