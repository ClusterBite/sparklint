from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

src = spark.readStream.table("raw")
q = src.writeStream.trigger(availableNow=True).option("checkpointLocation", "/c").toTable("t")
q2 = src.writeStream.trigger(processingTime="1 minute").option("checkpointLocation", "/c").start()


class Job:
    def trigger(self, once=False):
        pass


Job().trigger(once=True)
