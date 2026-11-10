from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import udf


@udf("boolean")
def is_valid(s):
    return "@" in s


check = F.udf(lambda x: x > 0, "boolean")
spark.udf.register("valid_email", lambda s: "@" in s, "boolean")

df = spark.table("users")
a = df.filter(is_valid(F.col("email")))  # expect: UDF002
b = df.where(check(df.amount) & (F.col("x") > 1))  # expect: UDF002
c = df.filter("valid_email(email)")  # expect: UDF002
d = df.filter(F.expr("VALID_EMAIL(email) AND x > 1"))  # expect: UDF002
