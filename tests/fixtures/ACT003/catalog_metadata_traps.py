from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Partition listings and commit history grow with the data: still reported.
parts = spark.sql("SHOW PARTITIONS sales").toPandas()  # expect: ACT003
history = spark.sql("DESCRIBE HISTORY sales").toPandas()  # expect: ACT003
# A metadata listing joined with a table is no longer metadata-sized.
big = spark.sql("SHOW TABLES").join(spark.table("t"), "tableName").toPandas()  # expect: ACT003
# Query text that merely mentions SHOW is a normal query.
rows = spark.sql("SELECT 'SHOW TABLES' AS s, * FROM t").toPandas()  # expect: ACT003
