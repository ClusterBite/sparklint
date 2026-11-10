from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

import logging

logger = logging.getLogger(__name__)
df = spark.read.parquet("/data")
print(df.count())  # expect: ACT007
logger.info("rows=%d", df.count())  # expect: ACT007
logging.info(f"rows: {df.count()}")  # expect: ACT007
df.count()  # expect: ACT007
