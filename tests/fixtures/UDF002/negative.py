from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import udf


@udf("string")
def norm(s):
    return s.strip()


df = spark.table("users")
a = df.filter(F.col("email").rlike("@"))
b = df.withColumn("n", norm("name")).filter(F.col("n") != "")
c = df.filter("email_valid = true")
nums = [1, 2, 3]
evens = list(filter(lambda x: x % 2 == 0, nums))
