# Overlap fixture: linted alone, this rule fires on every marked line; with ALL or the
# default preset a higher-precedence rule fires at the same site and this finding is
# dropped (sparklint_linter::overlap::PRECEDENCE; checked by the preset pass).
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()


@udf("boolean")
def fuzzy(a, b):
    return a[:3] == b[:3]


people = spark.table("people")
names = spark.table("names")

# UDF filter over a cross join reading both frames: UDF014 reports it
a = people.crossJoin(names).where(fuzzy(people.name, names.name))  # expect: UDF002
