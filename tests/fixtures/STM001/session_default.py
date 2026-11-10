from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# A session-level checkpoint default, set through a constant key.
CHECKPOINT_KEY = "spark.sql.streaming.checkpointLocation"
spark.conf.set(CHECKPOINT_KEY, f"/chk/{spark.sparkContext.appName}")
src = spark.readStream.table("raw")
q = src.writeStream.toTable("bronze")
