from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("account").orderBy("ts")
x = F.row_number().over(w)
y = F.sum("x").over(Window.partitionBy("k"))


class Window2:
    @staticmethod
    def orderBy(c):
        return c
