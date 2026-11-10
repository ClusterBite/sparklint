from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The replicated level only in a comment.
spark.sql("CACHE TABLE hot -- OPTIONS ('storageLevel' 'MEMORY_AND_DISK_2')")
spark.sql("SELECT 'CACHE TABLE t OPTIONS (storageLevel MEMORY_ONLY_2)' AS note")

LEVEL_SQL = "CACHE TABLE warm OPTIONS ('storageLevel' 'DISK_ONLY_2')"
spark.sql(LEVEL_SQL)  # expect: PRF029
