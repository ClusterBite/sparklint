from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Up to three keys: the recommended range
spark.sql("OPTIMIZE lake.events ZORDER BY (user_id)")
spark.sql("OPTIMIZE lake.events ZORDER BY (user_id, event_type, country)")
spark.sql("OPTIMIZE lake.events ZORDER BY user_id, ts")

# A function call with commas inside is one key
spark.sql("OPTIMIZE lake.events ZORDER BY (user_id, date_trunc('DAY', ts), country)")

# A backticked name holding commas is one key
spark.sql("OPTIMIZE lake.events ZORDER BY (`a,b,c,d`, e)")

# OPTIMIZE without ZORDER, liquid clustering
spark.sql("OPTIMIZE lake.events")
spark.sql("CREATE TABLE lake.t (a INT, b INT, c INT, d INT) CLUSTER BY (a, b, c, d)")

# The words only inside a string literal, a comment or an identifier
spark.sql("SELECT 'OPTIMIZE t ZORDER BY (a, b, c, d)' AS hint")
spark.sql("OPTIMIZE t ZORDER BY (a, b) -- was (a, b, c, d, e)")
spark.sql("SELECT zorder_by, a, b, c, d FROM ops.log")

# delta-spark builder within the limit, or an unknown column count
DeltaTable.forName(spark, "lake.events").optimize().executeZOrderBy("a", "b", "c")
dt = DeltaTable.forName(spark, "lake.events")
dt.optimize().executeZOrderBy(["a", "b"])
cols = load_columns()
dt.optimize().executeZOrderBy(*cols)
dt.optimize().executeZOrderBy(cols)
dt.optimize().executeCompaction()


# Not a DeltaTable / Spark session
class Maint:
    def optimize(self):
        return self

    def executeZOrderBy(self, *cols):
        return cols

    def sql(self, q):
        return q


m = Maint()
m.optimize().executeZOrderBy("a", "b", "c", "d")
m.sql("OPTIMIZE t ZORDER BY (a, b, c, d)")

# A plain string that is never run
doc = "OPTIMIZE t ZORDER BY (a, b, c, d)"


# Unknown query text
def run(q):
    return spark.sql(q)


# Suppressed
spark.sql("OPTIMIZE lake.wide ZORDER BY (a, b, c, d)")  # noqa: IOD020
