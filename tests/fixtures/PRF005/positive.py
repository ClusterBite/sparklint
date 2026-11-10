from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W

w = Window.orderBy("ts")
x = F.row_number().over(w)  # expect: PRF005
y = F.row_number().over(W.orderBy("ts"))  # expect: PRF005
