from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Capped streams.
a = (
    spark.readStream.format("kafka")
    .option("subscribe", "events")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 100000)
    .load()
)
b = spark.readStream.format("kafka").options(subscribe="t", maxOffsetsPerTrigger="5000").load()
c = spark.readStream.format("kafka").load(subscribe="t", MAXOFFSETSPERTRIGGER=10)
d = spark.readStream.format("kafka").option("subscribe", "t").option("maxBytesPerTrigger", "1g").load()

# Batch Kafka reads are bounded by endingOffsets, not micro-batches.
e = spark.read.format("kafka").option("subscribe", "t").option("startingOffsets", "earliest").load()

# Other streaming sources.
f = spark.readStream.format("delta").load("/tables/events")
g = spark.readStream.format("rate").load()

# Options from an opaque dict or **kwargs: the cap may be in there.
kafka_opts = {"subscribe": "t", "maxOffsetsPerTrigger": 1000}
h = spark.readStream.format("kafka").options(**kafka_opts).load()
i = spark.readStream.format("kafka").options(kafka_opts).load()

# Reader configured by a separate statement: unknown options.
r = spark.readStream.format("kafka")
r.option("maxOffsetsPerTrigger", 1000)
j = r.option("subscribe", "t").load()

# Unknown format.
fmt = "kafka" if spark else "delta"
k = spark.readStream.format(fmt).load()


# Not Spark: a user class with the same method names.
class Loader:
    def format(self, f):
        return self

    def load(self):
        return None


Loader().format("kafka").load()

# Suppressed.
m = spark.readStream.format("kafka").option("subscribe", "t").load()  # noqa: STM010
