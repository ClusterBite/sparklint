from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

spark = SparkSession.builder.getOrCreate()


@udf("boolean")
def fuzzy(a, b):
    return a[:3] == b[:3]


@pandas_udf("double")
def similarity(a: pd.Series, b: pd.Series) -> pd.Series:
    return a.str.len() - b.str.len()


close = F.udf(lambda x, y: abs(x - y) < 5, "boolean")
spark.udf.register("geo_near", lambda a, b, c, d: True, "boolean")

people = spark.table("people")
names = spark.table("names")

# UDF as the whole join condition.
a = people.join(names, fuzzy(people.name, names.name))  # expect: UDF014
b = people.join(names, on=fuzzy(people.name, names.name), how="left")  # expect: UDF014

# UDF compared with a literal / threshold.
c = people.join(names, similarity(people.name, names.name) > 0.8)  # expect: UDF014
d = people.join(names, fuzzy(people.name, names.name) == True)  # expect: UDF014

# UDF ANDed with a range test: still no equality key.
e = people.join(names, close(people.age, names.age) & (people.ts < names.ts))  # expect: UDF014

# Condition held in a variable / list form.
cond = fuzzy(people.name, names["name"])
f = people.join(names, cond, "inner")  # expect: UDF014
g = people.join(names, [close(people.age, names.age)])  # expect: UDF014

# Registered UDF called from SQL text.
h = people.join(names, F.expr("geo_near(p.lat, p.lon, n.lat, n.lon)"))  # expect: UDF014

# UDF ORed with an equality: the OR yields no key either.
i = people.join(names, fuzzy(people.name, names.name) | (people.id == names.id))  # expect: UDF014

# Filter over a cross join / condition-less join reading both frames.
j = people.crossJoin(names).where(fuzzy(people.name, names.name))  # expect: UDF014
k = people.join(names).filter(close(people["age"], names["age"]))  # expect: UDF014


def match(left: DataFrame, right: DataFrame) -> DataFrame:
    return left.join(right, fuzzy(left.k, right.k))  # expect: UDF014


# Small (broadcast) side: still reported, as info.
m = people.join(F.broadcast(names), fuzzy(people.name, names.name))  # expect: UDF014

for part in ["a", "b"]:
    n = people.join(names, close(people.age, names.age))  # expect: UDF014
