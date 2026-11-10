from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
orders = spark.read.table("orders")
customers = spark.read.table("customers")

# Other join strategy hints and partitioning hints.
j1 = orders.join(customers.hint("broadcast"), "customer_id")
j2 = orders.join(customers.hint("shuffle_hash"), "customer_id")
j3 = orders.join(customers.hint("merge"), "customer_id")
j4 = orders.hint("repartition", 8).join(customers, "customer_id")
j5 = orders.join(F.broadcast(customers), "customer_id")

# Hint names that only contain the text, or are not constant.
j6 = orders.hint("shuffle_replicate_nl_x")
j7 = orders.hint("no_shuffle_replicate_nl")


def pick(name):
    return orders.hint(name)


# SQL: other hints, ordinary comments, strings and identifiers.
q1 = spark.sql("SELECT /*+ BROADCAST(c) */ * FROM o JOIN c ON o.k = c.k")
q2 = spark.sql("SELECT /* SHUFFLE_REPLICATE_NL(c) */ * FROM o JOIN c ON o.k = c.k")
q3 = spark.sql("SELECT '/*+ SHUFFLE_REPLICATE_NL(c) */' AS txt FROM o")
q4 = spark.sql("SELECT shuffle_replicate_nl FROM hints_catalog")
q5 = spark.sql("SELECT * FROM o -- /*+ SHUFFLE_REPLICATE_NL(c) */\n JOIN c ON o.k = c.k")
q6 = spark.sql("SELECT /*+ MERGE(c), SHUFFLE_HASH(o) */ * FROM o JOIN c ON o.k = c.k")


def run(query):
    return spark.sql(query)


# Not Spark: a user class and plain strings.
class Planner:
    def hint(self, name):
        return name

    def sql(self, text):
        return text


p = Planner()
p.hint("shuffle_replicate_nl")
p.sql("SELECT /*+ SHUFFLE_REPLICATE_NL(c) */ 1")
doc = "use df.hint('shuffle_replicate_nl') only for theta joins"

# Suppressed.
j8 = orders.join(customers.hint("shuffle_replicate_nl"), F.col("a") < F.col("b"))  # noqa: PRF036
