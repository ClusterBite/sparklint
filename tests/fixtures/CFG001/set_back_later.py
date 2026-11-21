# Set back later: the earlier write is a scoped workaround (info).
spark.conf.set('spark.sql.adaptive.enabled', 'false')  # expect: CFG001[info]
x = 1
spark.conf.set('spark.sql.adaptive.enabled', 'true')
