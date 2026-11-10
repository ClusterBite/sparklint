from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

import logging

logger = logging.getLogger(__name__)
df = spark.read.parquet("/data")
items = [1, 2]
print(items.count(1))
catalog = get_catalog()
catalog.info("x")
n = df.count()
total = n * 2
logger.info("done")
spark.sql("SELECT 1 FROM t LIMIT 1").collect()
try:
    probe = spark.table("maybe_missing")
    probe.count()
except Exception:
    probe = None
