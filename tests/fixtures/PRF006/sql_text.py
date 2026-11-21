from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# An aggregating query read twice: reported.
counts = spark.sql("SELECT k, count(*) AS n FROM raw.e GROUP BY k")  # expect: PRF006
counts.write.mode("overwrite").saveAsTable("out.counts")
print(counts.count())

# Read from the clauses, not substrings: a column named `window`, `distinct_n`, a
# null-safe comparison and `INSERT OVERWRITE` are not a window, DISTINCT or OVER.
w = spark.sql("SELECT window, distinct_n FROM gold.windowed WHERE a IS NOT DISTINCT FROM b")
w.write.mode("overwrite").saveAsTable("out.w")
print(w.count())
