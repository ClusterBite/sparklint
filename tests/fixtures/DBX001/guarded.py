from pyspark import SparkContext
from pyspark.errors import PySparkNotImplementedError
from pyspark.sql import SparkSession
import pytest

spark = SparkSession.builder.getOrCreate()


# A fallback for sessions without a context: already portable, not reported.
def parallelism(session: SparkSession) -> int:
    try:
        if session and session.sparkContext:
            return session.sparkContext.defaultParallelism
        return 8
    except Exception:
        return 8


def resources(session: SparkSession):
    try:
        return session.sparkContext.resources
    except (KeyError, PySparkNotImplementedError):
        return session.conf.get("spark.executor.resource.gpu.amount")


try:
    ctx = SparkContext.getOrCreate()
except:  # noqa: E722
    ctx = None


def test_context_unavailable(shared: SparkSession):
    with pytest.raises(Exception):
        shared.sparkContext


# Still reported: a handler that raises again, a narrow handler, the handler arm,
# a function defined in the try body (runs later, unguarded).
try:
    sc_a = spark.sparkContext  # expect: DBX001
except Exception as e:
    print(e)
    raise

try:
    sc_b = spark.sparkContext  # expect: DBX001
except KeyError:
    sc_b = None

try:
    sc_c = None
except Exception:
    sc_c = spark.sparkContext  # expect: DBX001

try:

    def later():
        return spark.sparkContext  # expect: DBX001

except Exception:
    later = None

try:
    ctx2 = SparkContext.getOrCreate()  # expect: DBX001
finally:
    pass
