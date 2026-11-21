from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# One MERGE per day: N jobs, each re-reading `updates`.
def merge_days(updates: DataFrame, days):
    for d in days:
        (
            updates.filter(F.col("day") == d)
            .mergeInto("target", F.expr("t.id = s.id"))
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .merge()  # expect: ACT005
        )


# Traps: a writer built but never run in the loop, one merge after it.
def build_only(updates: DataFrame, days):
    for d in days:
        pending = updates.mergeInto("target", F.expr("t.id = s.id")).whenMatched().updateAll()
    updates.mergeInto("target", F.expr("t.id = s.id")).whenMatched().updateAll().merge()
