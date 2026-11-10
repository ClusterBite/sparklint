from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import rand, randn

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("events")

a = df.orderBy(F.rand()).limit(100)  # expect: PRF027
b = df.sort(rand(42)).limit(10)  # expect: PRF027
c = df.orderBy(F.rand().desc(), "id")  # expect: PRF027
d = df.orderBy([F.randn()])  # expect: PRF027
e = df.sort(F.desc(F.rand()))  # expect: PRF027
f = df.orderBy(F.expr("rand()"))  # expect: PRF027
g = df.orderBy(randn(seed=7).asc_nulls_last())  # expect: PRF027

h = spark.sql("SELECT * FROM events ORDER BY rand() LIMIT 1000")  # expect: PRF027

QUERY = """
WITH sample AS (
  SELECT * FROM events
  ORDER BY RANDOM()
  LIMIT 50
)
SELECT * FROM sample
"""
i = spark.sql(QUERY)  # expect: PRF027


def pick(frame: DataFrame, n: int) -> DataFrame:
    return frame.orderBy(F.rand()).limit(n)  # expect: PRF027


for day in ["2026-01-01", "2026-01-02"]:
    df.where(F.col("day") == day).orderBy(F.rand()).limit(5).show()  # expect: PRF027
