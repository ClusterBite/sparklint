spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)  # expect: CFG002[info]
spark.conf.unset('spark.sql.autoBroadcastJoinThreshold')
