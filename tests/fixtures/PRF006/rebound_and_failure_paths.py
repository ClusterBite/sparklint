from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# A name rebound on one branch: the later action runs once, through whichever
# binding is live.
def load(path, item_df=None):
    df = spark.read.csv(path, header=True)
    if item_df is not None:
        df = df.join(item_df, "item", "left")
    df.cache()
    df.count()
    return df


def sample_values(df, column, limit=None):
    values = df.select(column).distinct().orderBy(column)
    if limit is not None:
        values = values.limit(limit)
    return [row[0] for row in values.toLocalIterator()]


metrics = spark.sql("SELECT * FROM metrics ORDER BY ts")
combined = metrics
if flag:
    combined = metrics.join(spark.table("drift"), "ts")
combined.write.saveAsTable("combined")


# An assert message or a raise only runs when the check already failed.
def check_groups(df, col_name):
    distinct = df.select("group_id", col_name).distinct()
    groups = df.select("group_id").distinct().count()
    assert distinct.count() == groups, f"got {distinct.count()} for {groups} groups"


def check_empty(spark_df):
    orphans = spark_df.join(spark.table("parents"), "k", "left_anti")
    if not orphans.isEmpty():
        raise ValueError(f"{orphans.count()} orphan rows")
