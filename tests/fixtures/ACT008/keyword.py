from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

ids = [r.id for r in spark.table("ids").collect()]
# The query as the sqlQuery= keyword.
q = spark.sql(sqlQuery=f"SELECT * FROM t WHERE id IN ({','.join(str(i) for i in ids)})")  # expect: ACT008
