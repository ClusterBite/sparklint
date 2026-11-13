from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader

spark = SparkSession.builder.getOrCreate()


# A reader handed in by the caller, who may already have capped the batch size.
def start_from(reader: DataStreamReader):
    return reader.format("kafka").option("startingOffsets", "earliest").load()


# The reader chain starts here, so the missing cap is certain.
def start_here(session: SparkSession):
    return (
        session.readStream.format("kafka")
        .option("subscribe", "events")
        .option("startingOffsets", "earliest")
        .load()  # expect: STM010
    )
