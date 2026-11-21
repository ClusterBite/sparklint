from typing import Optional, Union
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()

dbl = lambda x: x * 2
by_name = udf(dbl)  # expect: UDF006
by_keyword = F.udf(f=lambda x: x + 1)  # expect: UDF006


@udf  # expect: UDF006
def optional_int(x) -> Optional[int]:
    return helper(x)


@udf  # expect: UDF006
def union_none(x) -> int | None:
    return helper(x)


@udf  # expect: UDF006
def union_typing(x) -> Union[None, float]:
    return helper(x)
