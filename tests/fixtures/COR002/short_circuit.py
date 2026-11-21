from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")
flag = True
opt = None


def check(msg: str | None, value: Column) -> Column:
    message = F.concat_ws("", F.lit("bad value"))
    # Ordinary short-circuit: Python never converts the last operand to bool.
    return msg or message


# A Column before the last operand is converted to bool: raises.
a = df.filter(F.col("a") > 1 and flag)  # expect: COR002[error] ~"'and' on Columns" help~"each comparison in parentheses"
b = df.filter((opt and F.col("a") > 1) or F.col("b") > 2)  # expect: COR002 ~"'or' on Columns"

# Only the last operand is a Column: the short-circuit idiom, not reported...
c = df.filter(flag and F.col("a") > 1)
d = opt and F.col("x") == opt
e = check(None, F.col("a"))
cols = [F.col(x) for x in ["a"] if x]

# ...unless the chain itself is a boolean test, which converts the Column.
col = F.col("a")
if col is not None and col == 1:  # expect: COR002[error] ~"This test gets a Column from 'and'" help~"Test Python values here"
    pass
while opt and F.col("a") > 1:  # expect: COR002 ~"from 'and'"
    break
assert flag or F.col("a")  # expect: COR002 ~"from 'or'"
f = 1 if opt and F.col("a") else 2  # expect: COR002 ~"from 'and'"
g = [x for x in [1] if opt and F.col("a") == x]  # expect: COR002 ~"from 'and'"
h = not (opt and F.col("a"))  # expect: COR002 ~"from 'and'" !~"'not' on a Column"
if flag and opt:
    pass
# Through a walrus and in a match guard the chain is the test, too.
if (t := opt and F.col("a") > 1):  # expect: COR002 ~"from 'and'"
    pass
match opt:
    case None if flag or F.col("a"):  # expect: COR002 ~"from 'or'"
        pass
