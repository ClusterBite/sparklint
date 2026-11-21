from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
src = spark.readStream.table("raw")

# Two triggers in one call: Spark raises "Multiple triggers not allowed", so the keyword
# swap would not fix it; reported without a fix (the snapshot equals the input).
w = src.writeStream.trigger(processingTime="0 seconds", once=True)  # expect: STM002
w2 = src.writeStream.trigger("10 seconds", once=True)  # expect: STM002
q = w.option("checkpointLocation", "/c").toTable("t")

# Not reported: once=False, availableNow, a non-literal flag.
ok1 = src.writeStream.trigger(once=False)
ok2 = src.writeStream.trigger(availableNow=True)
FLAG = True
ok3 = src.writeStream.trigger(once=FLAG)
