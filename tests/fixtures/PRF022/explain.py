# EXPLAIN only plans its query and returns the plan as one row of text:
# nothing is scanned, sorted or written, so rules about the query do not apply.
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def plan_text(df):
    df.createOrReplaceTempView("view")
    return spark.sql("explain cost select * from view").head(1)[0][0]


plan = spark.sql("EXPLAIN FORMATTED SELECT * FROM big").collect()
spark.sql("EXPLAIN SELECT * FROM big").write.saveAsTable("plans")

# Trap: the same query run for real.
rows = spark.sql("SELECT * FROM big").collect()  # expect: PRF022
spark.sql("SELECT * FROM big").write.saveAsTable("copy")  # expect: PRF022
