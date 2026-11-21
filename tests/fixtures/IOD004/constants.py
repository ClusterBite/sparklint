from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
MODE = "overwrite"

# A mode held in a constant; a None mode leaves the mode set before.
df.write.mode(MODE).insertInto("db.t")  # expect: IOD004 ~"insertInto(overwrite)"
df.write.mode("overwrite").mode(None).insertInto("db.t")  # expect: IOD004 ~"insertInto(overwrite)"
