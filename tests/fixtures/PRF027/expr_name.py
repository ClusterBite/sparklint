from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

# An F.expr string bound to a name once.
RANDOM_KEY = "rand(42)"
a = df.orderBy(F.expr(RANDOM_KEY)).limit(10)  # expect: PRF027

# A row bound that comes after the sort does not make the sort cheap.
b = df.orderBy(F.rand()).limit(10)  # expect: PRF027
