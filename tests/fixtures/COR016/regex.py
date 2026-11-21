import re
from re import sub as resub

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("t")
word = "x"

# Word boundaries that are backspaces.
a = df.filter(F.col("s").rlike("\bword\b"))  # expect: COR016[warning] ~"control character" help~"word boundary"
b = df.withColumn("s", F.regexp_replace("s", pattern="\bx\b", replacement=""))  # expect: COR016
c = re.compile(pattern="\bword\b")  # expect: COR016
d = df.filter(F.col("s").rlike(f"\b{word}\b"))  # expect: COR016 ~"control character"
e = df.filter(F.col("s").rlike(other="\bword"))  # expect: COR016
# Java reads a raw \v as a vertical-whitespace class, a plain "\v" is one tab character.
f = df.withColumn("s", F.regexp_replace("s", "\v", " "))  # expect: COR016

# Group references that are octal characters.
g = re.sub("(a)b", "\1", "ab")  # expect: COR016[warning] ~"group reference" ~"x01" help~"g<1>"
h = re.search("(a)\1", "aa")  # expect: COR016 ~"x01"
i = resub("(a)", "\2", "a")  # expect: COR016 ~"x02"
j = re.subn(repl="\1", pattern="(a)", string="a")  # expect: COR016
k = re.findall("(a)(b)\12", "ab")  # expect: COR016 ~"x0a"
m = df.filter(F.col("s").rlike("(a)\1"))  # expect: COR016

# Same meaning raw or not: not reported.
n1 = re.compile("\a\f")
n2 = re.compile("\0")
n3 = re.compile("\101")
n4 = re.compile("\v")
n5 = re.sub("x", "\b", "x")
n6 = re.sub("x", "\t\n", "x")
n7 = df.filter(F.col("s").rlike(r"\bword\b"))
n8 = re.sub(r"(a)", r"\1", "a")
n9 = re.sub("(a)", "\\1", "a")
n10 = df.withColumn("s", F.regexp_extract("s", rf"\b{word}", 0))
n11 = print("\b\1")
n12 = df.withColumn("s", F.regexp_replace("s", "x", "\1"))
