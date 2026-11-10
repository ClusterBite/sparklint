from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The session default spark.sql.streaming.checkpointLocation is only mentioned here,
# and in a string that is not a config write:
NOTE = "set spark.sql.streaming.checkpointLocation in the job config"
src = spark.readStream.table("raw")
q = src.writeStream.toTable("bronze")  # expect: STM001
