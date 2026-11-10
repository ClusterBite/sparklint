from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# A trailing comment is not part of the value.
spark.sql("SET spark.sql.adaptive.enabled = false -- AQE breaks this job")  # expect: CFG001
spark.sql("SELECT 'SET spark.sql.adaptive.enabled = false'")
