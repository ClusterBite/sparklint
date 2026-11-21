from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/data")

# Comparisons with NULL in SQL text are NULL for every row.
a = df.filter("a = NULL")  # expect: COR001[error] ~"`= NULL` in SQL text" ~"use IS NULL."
b = df.where("status <> null")  # expect: COR001 ~"`<> NULL`" ~"use IS NOT NULL."
c = df.filter(F.expr("a != NULL"))  # expect: COR001 ~"`!= NULL`" ~"IS NOT NULL"
d = df.select(F.expr("CASE WHEN a == NULL THEN 0 ELSE a END"))  # expect: COR001 ~"`== NULL`"
e = df.selectExpr("id", "NULL = flag AS missing")  # expect: COR001
f = spark.sql("SELECT * FROM t WHERE deleted_at = NULL")  # expect: COR001
g = spark.sql("SELECT * FROM t JOIN s ON t.k = s.k AND s.x <> NULL")  # expect: COR001
h = spark.sql("UPDATE t SET a = NULL WHERE b = NULL")  # expect: COR001 ~"use IS NULL."
i = spark.sql(
    "MERGE INTO t USING s ON t.id = s.id "  # expect: COR001
    "WHEN MATCHED AND s.deleted = NULL THEN UPDATE SET t.x = NULL"
)
cond = "a = NULL"
j = df.filter(cond)  # expect: COR001
# Both kinds in one text: one finding each.
k = df.filter("a = NULL OR b != NULL")  # expect: COR001, COR001
# A typed NULL literal is still NULL.
m = df.filter(F.col("a") == F.lit(None).cast("string"))  # expect: COR001 ~"Column == None"

# Not comparisons with NULL: not reported.
n1 = df.filter("a IS NULL")
n2 = df.filter("a IS NOT NULL AND b <=> NULL")
n3 = spark.sql("UPDATE t SET a = NULL, b = NULL WHERE c = 1")
n4 = spark.sql(
    "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.x = NULL"
)
n5 = spark.sql("SET spark.sql.some.key = NULL")
n6 = spark.sql("ALTER TABLE t SET TBLPROPERTIES ('note' = NULL)")
n7 = df.filter("note = 'a = NULL'")
n8 = df.filter("a = NULLIF(b, 0)")
n9 = df.selectExpr("coalesce(a, NULL) AS a")
n10 = F.col("a").eqNullSafe(F.lit(None))
n11 = print("a = NULL")
