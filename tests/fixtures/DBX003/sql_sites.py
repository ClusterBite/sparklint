from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")

# input_file_name() in SQL text: a selectExpr list, the str= / condition= keywords,
# a name bound to the text.
a = df.selectExpr(["id", "input_file_name() AS src"])  # expect: DBX003
b = df.withColumn("src", F.expr(str="input_file_name()"))  # expect: DBX003
c = df.where(condition="input_file_name() LIKE '%.csv'")  # expect: DBX003
SRC = "input_file_name() AS src"
d = df.selectExpr("id", SRC)  # expect: DBX003
