from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

spark = SparkSession.builder.getOrCreate()
bad = spark.table("bad_ids")
df = spark.table("events")
ids = [r.id for r in bad.collect()]

# Reported: other collect forms.
a1 = df.filter(F.col("id").isin([r.id for r in bad.toLocalIterator()]))  # expect: ACT008 ~"fed back into isin()" help~"left_semi"
s2 = {r.id for r in bad.collect()}
a2 = df.filter(F.col("id").isin(list(s2)))  # expect: ACT008
a3 = df.filter(F.col("id").isin(sorted(set(r.id for r in bad.collect()))))  # expect: ACT008
a4 = df.filter(~F.col("id").isin(ids))  # expect: ACT008
ids5 = bad.select("id").rdd.flatMap(lambda r: r).collect()
a5 = df.filter(F.col("id").isin(ids5))  # expect: ACT008

# Reported: values of a toPandas() column, also through the frame's name.
pdf = bad.toPandas()
a6 = df.filter(F.col("id").isin(list(pdf["id"])))  # expect: ACT008
a7 = df.filter(F.col("id").isin(pdf["id"].values.tolist()))  # expect: ACT008
a8 = df.filter(F.col("id").isin(pdf["id"].unique()))  # expect: ACT008

# Reported: collected values interpolated into a filter condition string.
a9 = df.filter(f"id IN ({','.join(map(str, ids))})")  # expect: ACT008 ~"into filter() SQL text"
a10 = df.where(f"id in {tuple(ids)}")  # expect: ACT008 ~"into where() SQL text"
a11 = df.filter(F.expr(f"id IN ({','.join(map(str, ids))})"))  # expect: ACT008 ~"into F.expr() SQL text"
a12 = df.select(expr(f"id IN ({','.join(map(str, ids))}) AS flagged"))  # expect: ACT008 ~"into expr() SQL text"

# Not reported: values that fit on the driver.
tables = [r.tableName for r in spark.sql("SHOW TABLES").collect()]
b1 = df.filter(F.col("t").isin(tables))
local = spark.createDataFrame([(1,), (2,)], ["id"])
b2 = df.filter(F.col("id").isin([r.id for r in local.collect()]))
top = [r.id for r in spark.sql("SELECT id FROM bad LIMIT 5").collect()]
b3 = df.filter(F.col("id").isin(top))
one = bad.agg(F.collect_set("id")).collect()[0][0]
b4 = df.filter(F.col("id").isin(one))
small = spark.table("t").limit(10).toPandas()
b5 = df.filter(F.col("id").isin(list(small["id"])))

# Not reported: column names, Python-side tests, constant condition strings.
b6 = df.filter(F.col("c").isin(list(pdf)))
b7 = df.filter(F.col("c").isin(list(pdf.columns)))
b8 = [x for x in range(10) if x in ids]
b9 = df.filter(f"day = '{'2024-01-01'}'")
b10 = df.filter("id IN (1, 2, 3)")
