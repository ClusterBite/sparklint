import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

import matplotlib.pyplot as plt

df = pd.read_csv("x.csv")
df.describe()
plt.show()
sdf = spark.table("t")
sdf.show()  # expect: ACT010
