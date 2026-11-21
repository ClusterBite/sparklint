from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

spark.udf.register("clean_name", lambda s: s.strip())
df = spark.table("users")
wanted = "bob"

# The condition text through a name, an f-string, F.expr(str=...).
COND = "clean_name(name) != ''"
a = df.filter(COND)  # expect: UDF002
b = df.where(f"clean_name(name) = '{wanted}'")  # expect: UDF002
c = df.filter(F.expr(str="clean_name(name) != ''"))  # expect: UDF002
