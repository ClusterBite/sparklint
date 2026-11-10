from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

src = spark.readStream.format("delta").table("raw")
q = src.writeStream.format("delta").outputMode("append").toTable("bronze")  # expect: STM001
q2 = src.writeStream.foreachBatch(lambda b, i: None).start()  # expect: STM001
w = src.writeStream.format("delta")
q3 = w.start("/out")  # expect: STM001
wopts = {"queryName": "q"}
q4 = src.writeStream.options(**wopts).start("/out")  # expect: STM001
