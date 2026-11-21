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
big.join(local, big.ts >= local.id)  # expect: PRF004[info]
rows.join(big, big.ts >= rows.id)  # expect: PRF004[info]
big.join(local.join(big, "id"), big.ts >= local.id)  # expect: PRF004[warning]
