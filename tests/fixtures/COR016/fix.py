from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

pattern = "\w+@\w+"  # expect: COR016
both = "\d\n"  # expect: COR016
