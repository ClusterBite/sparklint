import pyspark.pandas as ps

ON = True
ps.set_option("compute.ops_on_diff_frames", ON)  # expect: PDS005
