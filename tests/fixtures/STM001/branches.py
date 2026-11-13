from pyspark.sql import SparkSession

from helpers import make_writer

spark = SparkSession.builder.getOrCreate()
src = spark.readStream.table("raw")
mode = spark.conf.get("mode", "a")

# A writer built on several paths, each a fresh `df.writeStream`: what every path
# sets is known, so a checkpoint missing on all of them is reported.
if mode == "a":
    w = src.writeStream.format("delta")
else:
    w = src.writeStream.format("delta").option("mergeSchema", True)
q = w.start("/out")  # expect: STM001

w3 = src.writeStream.format("delta") if mode == "a" else src.writeStream.format("parquet")
q3 = w3.start("/out3")  # expect: STM001

# Set on one path: not reported.
if mode == "a":
    w2 = src.writeStream.format("delta").option("checkpointLocation", "/chk/2")
else:
    w2 = src.writeStream.format("delta")
q2 = w2.start("/out2")

# One path of unknown origin: the helper may set it.
w4 = make_writer(src) if mode == "a" else src.writeStream
q4 = w4.start("/out4")
