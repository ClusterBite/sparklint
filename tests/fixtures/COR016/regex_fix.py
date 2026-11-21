import re

from pyspark.sql import functions as F

a = re.sub("(\d+)-(\w+)", "\2-\1", "1-a")  # expect: COR016, COR016
b = re.compile("\bword\b")  # expect: COR016
c = re.sub("(a)", "\1\n", "a")  # expect: COR016
d = F.regexp_extract("s", "\btoken\b", 0)  # expect: COR016
# A line continuation inside the pattern: a raw prefix would keep the backslash and the
# line break in the pattern, so no fix.
e = re.search("start \
\bend", "s")  # expect: COR016
