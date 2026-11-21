from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

src = spark.read.parquet("/updates")

# Spark 4 DataFrame.mergeInto: the write runs at `.merge()`, after the when-clauses.
(
    src.mergeInto("target", F.expr("t.id = s.id"))
    .whenMatched()
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .merge()  # expect: ACT001
)
src.mergeInto("target", F.expr("t.id = s.id")).whenMatched("s.deleted").delete().merge()  # expect: ACT001
(
    src.mergeInto("target", F.expr("t.id = s.id"))
    .withSchemaEvolution()
    .whenNotMatchedBySource()
    .delete()
    .merge()  # expect: ACT001
)

# Building the writer without `.merge()` runs nothing.
pending = src.mergeInto("target", F.expr("t.id = s.id")).whenMatched().updateAll()
