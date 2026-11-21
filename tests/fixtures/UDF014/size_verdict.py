import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

big = spark.table("events")
pdf = pd.DataFrame({"id": [1, 2, 3]})
local = spark.createDataFrame(pdf)
rows = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
tables = spark.sql("SHOW TABLES IN sales")

near = F.udf(lambda x, y: abs(x - y) < 5, "boolean")
# A side that fits the driver lowers the finding to info.
big.join(local, near(big.x, local.id))  # expect: UDF014[info]
rows.crossJoin(big).where(near(big.x, rows.id))  # expect: UDF014[info]
big.join(big, near(big.x, big.y))  # expect: UDF014[warning]
