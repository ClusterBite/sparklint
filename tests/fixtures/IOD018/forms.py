import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Unity Catalog metadata sync, not a partition listing.
spark.sql("REPAIR TABLE lake.t SYNC METADATA")

# The statement text inside a string literal of another query.
spark.sql("SELECT 'MSCK REPAIR TABLE x' AS note")

# ADD / DROP PARTITIONS still walk the table directory.
spark.sql("REPAIR TABLE lake.x ADD PARTITIONS")  # expect: IOD018
spark.sql("REPAIR TABLE lake.x DROP PARTITIONS")  # expect: IOD018

# Retry loop: the repair runs once (still reported, at the plain level).
for attempt in range(3):
    try:
        spark.sql("MSCK REPAIR TABLE lake.events")  # expect: IOD018
        break
    except Exception:
        time.sleep(5)
