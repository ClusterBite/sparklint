from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# A loop over a short constant list is a small driver loop (info), however the
# list is bound: annotated with a generic type, parenthesised, chained.
TABLES: dict[str, int] = {"a": 1, "b": 2}
for t in TABLES:
    spark.table("events").count()  # expect: ACT005[info]

DAYS = (
    "mon",
    "tue",
)
for d in DAYS:
    spark.table("events").count()  # expect: ACT005[info]

FIRST = SECOND = ["x", "y"]
for s in FIRST:
    spark.table("events").count()  # expect: ACT005[info]

# Not a constant: a parameter.
def run(names):
    for n in names:
        spark.table("events").count()  # expect: ACT005[warning]
