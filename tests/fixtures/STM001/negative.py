from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

src = spark.readStream.format("delta").table("raw")
q = src.writeStream.option("checkpointLocation", "/chk/a").toTable("bronze")
q2 = src.writeStream.format("delta").start("/out", checkpointLocation="/chk/b")
q3 = src.writeStream.options(checkpointLocation="/chk/c").start()
q4 = src.writeStream.format("console").start()
opts = {"checkpointLocation": "/chk/d"}
q5 = src.writeStream.options(**opts).start()
w = src.writeStream.option("checkpointLocation", "/chk/e")
q6 = w.start()


class Writer:
    def __init__(self, df):
        self._jwrite = df._jdf.writeStream()

    def start(self):
        return self._jwrite.start()
