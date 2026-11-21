# A restore BEFORE the disable does not count.
spark.conf.set('spark.sql.adaptive.enabled', 'true')
spark.conf.set('spark.sql.adaptive.enabled', 'false')  # expect: CFG001[warning]
