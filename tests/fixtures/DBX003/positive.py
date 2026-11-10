from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import input_file_name

df = spark.read.json("/landing")
a = df.withColumn("src", F.input_file_name())  # expect: DBX003
b = df.withColumn("src", input_file_name())  # expect: DBX003
c = df.selectExpr("*", "input_file_name() AS src")  # expect: DBX003
d = spark.sql("SELECT input_file_name() FROM json.`/x`")  # expect: DBX003
