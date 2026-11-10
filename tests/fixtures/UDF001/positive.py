from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

lower_udf = udf(lambda x: x.lower())  # expect: UDF001
upper_udf = F.udf(lambda x: x.upper(), StringType())  # expect: UDF001
trim_udf = udf(lambda x: x.strip())  # expect: UDF001
len_udf = udf(lambda x: len(x))  # expect: UDF001
abs_udf = udf(lambda x: abs(x))  # expect: UDF001
null_udf = udf(lambda x: x is None)  # expect: UDF001


@udf(returnType=StringType())  # expect: UDF001
def lower_dec(s):
    return s.lower()
