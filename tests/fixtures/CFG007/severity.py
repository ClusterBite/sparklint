spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '2g')  # expect: CFG007[warning]
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '8g')  # expect: CFG007[error]
