from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("SET spark.sql.streaming.checkpointLocation = /chk/root")
src = spark.readStream.table("raw")
q = src.writeStream.toTable("bronze")
