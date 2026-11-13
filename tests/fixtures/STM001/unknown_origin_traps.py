from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()


# The DataFrame is a parameter, but the writer chain starts here: every option is visible.
def write_stream(df: DataFrame, location: str):
    return df.writeStream.format("delta").start(location)  # expect: STM001


src = spark.readStream.table("raw")
# A stored writer followed through two names.
w = src.writeStream.format("delta")
w2 = w.outputMode("append")
q = w2.toTable("bronze")  # expect: STM001
# A reader stored in a name, then the stream written without a checkpoint.
r = spark.readStream
events = r.format("delta").load("/events")
q2 = events.writeStream.start("/out")  # expect: STM001
