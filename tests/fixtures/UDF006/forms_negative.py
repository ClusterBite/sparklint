from typing import Optional, Union
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()


# String results stay strings: an Optional[str] / str | None annotation is not reported.
@udf
def optional_str(x) -> Optional[str]:
    return helper(x)


@udf
def str_or_none(x) -> str | None:
    return helper(x)


@udf
def union_str(x) -> Union[str, None]:
    return helper(x)


to_text = lambda x: str(x)
by_name = udf(to_text)
by_keyword = F.udf(f=lambda x: x.strip())
typed_keyword = F.udf(f=lambda x: x + 1, returnType="int")
none_only = udf(lambda x: None)
