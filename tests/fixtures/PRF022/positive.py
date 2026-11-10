from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

spark.sql("SELECT * FROM raw.events").write.saveAsTable("copy")  # expect: PRF022
df = spark.sql("select * from t")  # expect: PRF022
df.write.parquet("/p")
rows = spark.sql(f"SELECT * FROM {tbl}").collect()  # expect: PRF022
