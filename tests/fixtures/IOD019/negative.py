import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data/in")

# Large or unlimited caps.
df.write.option("maxRecordsPerFile", 1000000).parquet("/data/out")
df.write.option("maxRecordsPerFile", 100000).parquet("/data/out")
df.write.option("maxRecordsPerFile", 0).parquet("/data/out")
df.write.option("maxRecordsPerFile", -1).parquet("/data/out")
spark.conf.set("spark.sql.files.maxRecordsPerFile", 5000000)
spark.conf.set("spark.sql.files.maxRecordsPerFile", "0")

# Unknown values.
df.write.option("maxRecordsPerFile", os.environ["CAP"]).parquet("/data/out")
df.write.option("maxRecordsPerFile", "10k").parquet("/data/out")
spark.conf.set("spark.sql.files.maxRecordsPerFile", os.environ["CAP"])


def write(frame, cap):
    frame.write.option("maxRecordsPerFile", cap).parquet("/data/out")


# Other keys / reads / gets.
df.write.option("maxPartitionBytes", 1000).parquet("/data/out")
spark.read.option("maxRecordsPerFile", 1000).parquet("/data/in")
spark.conf.get("spark.sql.files.maxRecordsPerFile")
spark.conf.unset("spark.sql.files.maxRecordsPerFile")
spark.conf.set("spark.sql.files.maxPartitionBytes", 1000)
spark.sql("SELECT 'spark.sql.files.maxRecordsPerFile = 1000'")

# Builder option not written by this terminal.
writer = df.write.option("maxRecordsPerFile", 1000)

# Look-alikes.
settings = {"maxRecordsPerFile": 1000}


class Sink:
    def option(self, k, v):
        return self

    def save(self, p):
        pass


Sink().option("maxRecordsPerFile", 1000).save("/x")

df.write.option("maxRecordsPerFile", 1000).parquet("/data/out")  # noqa: IOD019
