import re

from pyspark.sql import functions as F

rev = F.udf(lambda s: s[::-1])  # expect: UDF001
last3 = F.udf(lambda s: s[-3:])  # expect: UDF001
size = F.udf(lambda xs: len(xs))  # expect: UDF001
starts = F.udf(lambda s: bool(re.match(r"^a", s)), "boolean")  # expect: UDF001
has_x = F.udf(lambda s: re.search(r"x", s) is not None, "boolean")  # expect: UDF001
no_x = F.udf(lambda s: re.search(r"x", s) is None, "boolean")  # expect: UDF001

# Not a built-in: a slice with another step, a match group, a bool of something else.
odd = F.udf(lambda s: s[::2])
every_other = F.udf(lambda s: s[1::2])
group = F.udf(lambda s: re.match(r"(a+)", s).group(1))
pattern = re.compile(r"x")
compiled = F.udf(lambda s: pattern.search(s) is not None, "boolean")
nonempty = F.udf(lambda s: bool(s.split(",")), "boolean")
