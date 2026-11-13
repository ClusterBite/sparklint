from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

for r in spark.sql("SHOW PARTITIONS sales").collect():  # expect: ACT004
    print(r)

for h in spark.sql("DESCRIBE HISTORY sales").collect():  # expect: ACT004
    print(h.version)

for row in spark.sql("SELECT * FROM sales").collect():  # expect: ACT004
    print(row)
