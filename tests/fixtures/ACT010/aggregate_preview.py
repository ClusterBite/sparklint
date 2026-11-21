from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
big = spark.table("events")

# A few rows out of an aggregate that reads the whole frame: still a full job.
big.agg(F.max("id")).show()  # expect: ACT010[warning]
big.describe().show()  # expect: ACT010[warning]
big.summary().show()  # expect: ACT010[warning]
big.groupBy().count().show()  # expect: ACT010[warning]
display(big.agg(F.max("id")))  # expect: ACT010[warning]
display(big.describe("id"))  # expect: ACT010[warning]
spark.sql("SELECT count(*) FROM events").show()  # expect: ACT010[warning]
spark.sql("SELECT count(*) AS n FROM events LIMIT 1").show()  # expect: ACT010[warning]

# A limit before the aggregate, metadata or a table-less query: a small look.
big.limit(100).agg(F.max("id")).show()
big.limit(10).describe().show()
spark.sql("SELECT * FROM events LIMIT 5").show()
spark.sql("DESCRIBE DETAIL events").show()
spark.sql("SELECT current_date()").show()
spark.sql("EXPLAIN SELECT count(*) FROM events").show()
