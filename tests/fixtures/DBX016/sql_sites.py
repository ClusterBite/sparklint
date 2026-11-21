from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The SQL filter text through a name.
IS_ORDERS = "tableName = 'orders'"
m = spark.sql("SHOW TABLES IN sales").where(IS_ORDERS).collect()  # expect: DBX016
