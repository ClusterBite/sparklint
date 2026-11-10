from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()

servers = "broker:9092"

# Earliest offsets, no cap: the first batch reads the whole topic (warning).
a = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", servers)
    .option("subscribe", "events")
    .option("startingOffsets", "earliest")
    .load()  # expect: STM010
)

# Explicit offsets map (JSON) also starts from a fixed backlog (warning).
b = (
    spark.readStream.format("kafka")
    .options(subscribe="events", startingOffsets='{"events": {"0": 0}}')
    .load()  # expect: STM010
)

# Default (latest) start: info, a restart after downtime still drains the backlog.
c = spark.readStream.format("kafka").option("subscribe", "events").load()  # expect: STM010

# Format given to load(); upper-case format name; key case-insensitive.
d = spark.readStream.option("subscribe", "events").load(format="kafka")  # expect: STM010
e = spark.readStream.format("KAFKA").option("STARTINGOFFSETS", "Earliest").load()  # expect: STM010

# Reader bound to a name first, configured in the same chain.
reader = spark.readStream.format("kafka").option("subscribe", "events")
f = reader.option("startingOffsets", "earliest").load()  # expect: STM010

# Keyword options on load().
g = spark.readStream.format("kafka").load(subscribe="events", startingOffsets="earliest")  # expect: STM010


def read_topic(session: SparkSession, topic: str) -> DataFrame:
    return session.readStream.format("kafka").option("subscribe", topic).load()  # expect: STM010


for topic in ["a", "b"]:
    spark.readStream.format("kafka").option("subscribe", topic).load()  # expect: STM010

kafka_opts = {"subscribe": "t"}
h = spark.readStream.format("kafka").options(**kafka_opts).load()  # expect: STM010
