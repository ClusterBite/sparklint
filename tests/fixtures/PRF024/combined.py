from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Several CUBE / ROLLUP / GROUPING SETS items of one GROUP BY multiply their sets.
q1 = spark.sql("SELECT 1 FROM t GROUP BY CUBE(a, b, c), CUBE(d, e, f)")  # expect: PRF024
q2 = spark.sql(  # expect: PRF024
    """
    SELECT region, store, y, m, d, sum(x)
    FROM t
    GROUP BY CUBE(region, store, y), ROLLUP(m, d, h)
    """
)
q3 = spark.sql(  # expect: PRF024
    "SELECT 1 FROM t GROUP BY CUBE(a, b, c, d), GROUPING SETS ((e), (f), ())"
)

for day in ["mon", "tue"]:
    spark.sql(f"SELECT 1 FROM t_{day} GROUP BY CUBE(a, b, c, d, e)")  # expect: PRF024

# Traps: the product stays below a 5-column CUBE's 32 sets.
n1 = spark.sql("SELECT 1 FROM t GROUP BY CUBE(a, b), ROLLUP(c, d, e)")
n2 = spark.sql("SELECT 1 FROM t GROUP BY ROLLUP(a, b), ROLLUP(c, d), ROLLUP(e)")
# Two small CUBEs in different GROUP BY clauses do not multiply.
n3 = spark.sql(
    "SELECT 1 FROM (SELECT a FROM t GROUP BY CUBE(a, b, c)) GROUP BY CUBE(d, e, f)"
)
# Plain grouping columns next to a CUBE add no sets.
n4 = spark.sql("SELECT 1 FROM t GROUP BY a, b, c, d, CUBE(e, f)")
