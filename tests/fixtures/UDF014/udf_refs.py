from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

fuzzy = F.udf(lambda a, b: a[:3] == b[:3], "boolean")

people = spark.table("people")
names = spark.table("names")

# A UDF behind a Column variable, compared with a literal: no join key.
same = fuzzy(people.name, names.name)
a = people.join(names, same == F.lit(True))  # expect: UDF014
one_side = fuzzy(people.name, people.alias)
b = people.join(names, (one_side == F.lit(True)) & (people.k > names.k))  # expect: UDF014

# With an equality key next to it the UDF only filters the matched pairs.
c = people.join(names, (one_side == F.lit(True)) & (people.k == names.k))
