from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# A start timestamp replays everything since that time in the first batch (warning).
a = (
    spark.readStream.format("kafka")
    .option("subscribe", "events")
    .option("startingTimestamp", "1700000000000")
    .load()  # expect: STM010
)
b = spark.readStream.format("kafka").option("startingOffsetsByTimestamp", '{"events": {"0": 1}}').load()  # expect: STM010
c = spark.readStream.load(format="kafka", subscribe="events", startingTimestamp="1700000000000")  # expect: STM010

# minOffsetsPerTrigger only delays small batches; it does not cap a backlog.
d = spark.readStream.format("kafka").option("minOffsetsPerTrigger", 1000).load()  # expect: STM010

# Not reported: capped timestamp starts, batch reads.
ok1 = (
    spark.readStream.format("kafka")
    .option("startingTimestamp", "1700000000000")
    .option("maxOffsetsPerTrigger", 500_000)
    .load()
)
ok2 = spark.readStream.format("kafka").options(startingOffsetsByTimestamp="{}", MaxBytesPerTrigger="1g").load()
ok3 = spark.read.format("kafka").option("startingTimestamp", "1700000000000").load()
