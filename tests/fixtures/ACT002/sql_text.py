from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The query is read once: positional or sqlQuery=, literal or a name bound to one.
TOP = "SELECT * FROM events ORDER BY ts DESC LIMIT 10"
top = spark.sql(TOP).collect()
latest = spark.sql(sqlQuery="SELECT max(ts) FROM events").collect()

# A LIMIT in a trailing comment or inside a string does not bound the result.
commented = spark.sql("SELECT * FROM events -- LIMIT 1").collect()  # expect: ACT002
quoted = spark.sql("SELECT * FROM events WHERE note = 'LIMIT 1'").collect()  # expect: ACT002
