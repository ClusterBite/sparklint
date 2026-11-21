from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# The registered name as a keyword, or held in a constant.
spark.udf.register(name="clean_name", f=lambda s: s.strip(), returnType="string")
UDF_NAME = "mask_card"
spark.udf.register(UDF_NAME, lambda s: s[-4:], "string")
df = spark.table("users")

a = df.filter("clean_name(name) != ''")  # expect: UDF002
b = df.where("mask_card(card) = '1234'")  # expect: UDF002
