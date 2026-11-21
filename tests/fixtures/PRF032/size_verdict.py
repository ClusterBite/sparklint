import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("events")
pdf = pd.DataFrame({"id": [1, 2, 3]})
local = spark.createDataFrame(pdf)
rows = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
tables = spark.sql("SHOW TABLES IN sales")

# A side that fits the driver lowers the finding to info.
big.join(local, (big.id == local.id) | (big.v == local.v))  # expect: PRF032[info]
big.join(big, (big.id == big.k) | (big.v == big.w))  # expect: PRF032[warning]
