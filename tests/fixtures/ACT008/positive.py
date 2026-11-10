from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

bad = spark.table("bad_ids")
df = spark.table("events")
ids = [r.id for r in bad.select("id").collect()]
out = df.filter(F.col("id").isin(ids))  # expect: ACT008

rows = bad.select("id").distinct().collect()
keys = [r[0] for r in rows]
out2 = df.where(df.id.isin(*keys))  # expect: ACT008

vals = bad.toPandas()["id"].tolist()
out3 = df.filter(F.col("id").isin(vals))  # expect: ACT008

q = spark.sql(f"SELECT * FROM t WHERE id IN ({','.join(str(i) for i in ids)})")  # expect: ACT008
