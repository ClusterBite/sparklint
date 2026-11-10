import sqlite3

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Literal lists, IN (SELECT ...) and NOT EXISTS are fine.
a = spark.sql("SELECT * FROM a WHERE id NOT IN (1, 2, 3)")
b = spark.sql("SELECT * FROM a WHERE id IN (SELECT id FROM b)")
c = spark.sql("SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id)")
d = spark.sql("SELECT * FROM a LEFT ANTI JOIN b ON a.id = b.id")

# The pattern only inside a string literal or a comment.
e = spark.sql("SELECT 'id NOT IN (SELECT id FROM b)' AS note FROM t")
f = spark.sql("""
SELECT a  -- was: WHERE id NOT IN (SELECT id FROM b)
FROM t /* NOT IN (SELECT 1) */
""")

# Identifiers that merely contain the words.
g = spark.sql("SELECT cannot_in FROM t WHERE knot IN (SELECT k FROM s)")

# Not a SparkSession: sqlite / user objects with a .sql method.
conn = sqlite3.connect(":memory:")
conn.execute("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)")


class Db:
    def sql(self, q):
        return q


Db().sql("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)")


def untyped(s):
    # Unknown receiver: not reported.
    return s.sql("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)")


# Dynamic text that cannot be resolved statically.
def dyn(q):
    return spark.sql(q)


ok = spark.sql("SELECT * FROM a WHERE id NOT IN (SELECT id FROM b)")  # noqa: PRF025
