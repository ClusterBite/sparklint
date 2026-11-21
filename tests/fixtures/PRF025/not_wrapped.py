from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# NOT (x IN (subquery)) is planned exactly like x NOT IN (subquery).
a = spark.sql("SELECT * FROM a WHERE NOT (id IN (SELECT id FROM b))")  # expect: PRF025
b = spark.sql(  # expect: PRF025
    """
    SELECT * FROM orders o
    WHERE NOT ( (o.k1, o.k2) IN (SELECT k1, k2 FROM returns) )
    """
)

# Traps: literal list, a NOT over a whole condition, NOT EXISTS.
c = spark.sql("SELECT * FROM a WHERE NOT (id IN (1, 2, 3))")
d = spark.sql("SELECT * FROM a WHERE NOT (id IN (SELECT id FROM b) AND x > 1)")
e = spark.sql("SELECT * FROM a WHERE NOT (EXISTS (SELECT 1 FROM b WHERE b.id = a.id))")
f = spark.sql("SELECT 'NOT (id IN (SELECT id FROM b))' AS note FROM t")
