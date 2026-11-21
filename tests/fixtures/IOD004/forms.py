from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
OVERWRITE = True

# Overwrite flag held in a constant, mode given by keyword.
df.write.insertInto("db.t", OVERWRITE)  # expect: IOD004 ~"insertInto(overwrite)"
df.write.mode(saveMode="overwrite").insertInto("db.t")  # expect: IOD004 ~"insertInto(overwrite)"

# overwrite=None leaves the mode alone; overwrite=False appends whatever the mode.
df.write.insertInto("db.t", overwrite=None)  # expect: IOD004 ~"insertInto() matches"
df.write.mode("overwrite").insertInto("db.t", overwrite=None)  # expect: IOD004 ~"insertInto(overwrite)"
df.write.mode("overwrite").insertInto("db.t", overwrite=False)  # expect: IOD004 ~"insertInto() matches"

# Spark rejects insertInto after partitionBy / bucketBy.
df.write.partitionBy("d").insertInto("db.t")  # expect: IOD004[error] ~"partitionBy()" help~"Drop the partitionBy() call"
df.write.bucketBy(4, "k").insertInto("db.t")  # expect: IOD004[error] ~"bucketBy()"
w = df.write.mode("append").partitionBy("d")
w.insertInto("db.t")  # expect: IOD004[error] ~"partitionBy()"


# A writer typed by its annotation.
def save(wr: DataFrameWriter):
    wr.insertInto("db.t")  # expect: IOD004
