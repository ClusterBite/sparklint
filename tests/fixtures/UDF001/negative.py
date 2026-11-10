from pyspark.sql.functions import udf
import phonenumbers

custom_udf = udf(lambda x: x[:3] + "-" + x[3:])
parse_udf = udf(lambda x: phonenumbers.parse(x).country_code)
lower = lambda s: s.lower()
names = map(lambda s: s.upper(), ["a"])
