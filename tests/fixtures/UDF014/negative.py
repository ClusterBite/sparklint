from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.getOrCreate()


@udf("boolean")
def fuzzy(a, b):
    return a[:3] == b[:3]


@udf("string")
def norm(s):
    return s.strip().lower()


spark.udf.register("geo_near", lambda a, b, c, d: True, "boolean")

people = spark.table("people")
names = spark.table("names")

# An equality key next to the UDF: Spark hash-joins on it, then applies the UDF.
a = people.join(names, (people.zip == names.zip) & fuzzy(people.name, names.name))
b = people.join(names, [people.zip == names.zip, fuzzy(people.name, names.name)])
c = people.join(names, F.expr("p.zip = n.zip AND geo_near(p.lat, p.lon, n.lat, n.lon)"))
key = people.zip == names.zip
d = people.join(names, key & fuzzy(people.name, names.name))

# A one-frame UDF on one side of an equality is still a usable key.
e = people.join(names, norm(people.name) == names.name_norm)

# Plain joins.
f = people.join(names, "id")
g = people.join(names, ["id", "zip"], "left")
h = people.join(names, people.id == names.id)

# Built-ins in the condition (PRF004's domain, not a UDF).
i = people.join(names, F.levenshtein(people.name, names.name) < 3)

# Opaque helper / unresolved variable next to the UDF: may be the key.
def _key(x, y):
    return x == y


j = people.join(names, _key(people.id, names.id) & fuzzy(people.name, names.name))


def build(c1, extra):
    return people.join(names, extra & fuzzy(people.name, names.name))


# UDF filter over a cross join that reads only one frame (pushed below the join).
k = people.crossJoin(names).where(fuzzy(people.name, F.lit("bob")))
# Filter after a keyed join: evaluated per joined row, not per pair (UDF002's).
l = people.join(names, "id").filter(fuzzy(people.name, names.name))
# Filter with an equality key: the optimizer turns it into an equi-join.
m = people.crossJoin(names).where((people.id == names.id) & fuzzy(people.name, names.name))

# SQL text calling an unregistered function.
n = people.join(names, F.expr("other_fn(p.a, n.b)"))


# Not Spark: user class with join / filter.
class Table:
    def join(self, other, on):
        return self

    def filter(self, cond):
        return self


t = Table()
o = t.join(t, fuzzy(1, 2))
p = "".join(["a", "b"])

q = people.join(names, fuzzy(people.name, names.name))  # noqa: UDF014
