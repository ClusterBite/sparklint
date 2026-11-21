# broadcast-threshold-max = "0" (sparklint.toml) turns CFG007 off.
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '16g')
