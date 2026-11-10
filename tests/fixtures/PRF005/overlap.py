# Overlap fixture: linted alone, this rule fires on every marked line; with ALL or the
# default preset a higher-precedence rule fires at the same site and this finding is
# dropped (sparklint_linter::overlap::PRECEDENCE; checked by the preset pass).
from pyspark.sql import Window
from pyspark.sql import functions as F

# running collect_list over an unpartitioned window: PRF031 reports it
c = F.collect_list("v").over(Window.orderBy("ts"))  # expect: PRF005
