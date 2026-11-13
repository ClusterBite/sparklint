from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamWriter

spark = SparkSession.builder.getOrCreate()


# The writer comes from the caller, who may have set checkpointLocation.
def write_stream(writer: DataStreamWriter, location: str):
    if location.startswith("/"):
        return writer.start(location)
    return writer.toTable(location)


# Built by a helper we do not follow.
def make_writer(df: DataFrame) -> DataStreamWriter:
    return df.writeStream.option("checkpointLocation", "/chk/helper")


src = spark.readStream.table("raw")
q = make_writer(src).format("delta").toTable("bronze")

# Bound on two paths: one of them sets the checkpoint.
if spark.conf.get("x", "a") == "a":
    w = src.writeStream.option("checkpointLocation", "/chk/a")
else:
    w = src.writeStream.option("checkpointLocation", "/chk/b")
q2 = w.start("/out")
