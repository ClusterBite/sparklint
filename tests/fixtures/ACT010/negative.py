from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

import pandas as pd

df = spark.read.parquet("/data")
pdf = pd.DataFrame({"a": [1]})
pdf.describe()
widget = get_widget()
widget.show()
df.write.parquet("/out")


def dbutils_shim():
    try:
        import IPython

        return IPython.get_ipython()
    except AttributeError:
        from IPython.display import display

        class Widgets:
            def text(self, w):
                display(w)

        return Widgets()
