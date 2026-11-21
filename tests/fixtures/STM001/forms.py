from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
src = spark.readStream.format("delta").table("raw")

# Positional path / format / output mode carry no checkpoint.
a = src.writeStream.start("/out", "delta", "append")  # expect: STM001
# foreachBatch sinks need a checkpoint too.
b = src.writeStream.trigger(availableNow=True).foreachBatch(print).start()  # expect: STM001

# Not reported: option keys are case-insensitive, keyword options on toTable / start,
# a checkpoint from a parameter, debug sinks (memory, noop, console given to start()).


def run(ckpt):
    ok1 = src.writeStream.option("CHECKPOINTLOCATION", "/c/a").start()
    ok2 = src.writeStream.toTable("t", checkpointLocation="/c/b")
    ok3 = src.writeStream.option("checkpointLocation", ckpt).start()
    ok4 = src.writeStream.format("noop").start()
    ok5 = src.writeStream.start(format="console")
    return ok1, ok2, ok3, ok4, ok5
