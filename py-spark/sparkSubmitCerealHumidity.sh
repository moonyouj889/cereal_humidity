spark-submit --master yarn --packages org.apache.spark:spark-avro_2.11:2.4.4 \
--jars /usr/local/shc/core/target/shc-core-1.1.3-2.4-s_2.11.jar,\
$HBASE_HOME/lib/hbase-client*.jar,\
$HBASE_HOME/lib/hbase-common*.jar,\
$HBASE_HOME/lib/hbase-server*.jar,\
$HBASE_HOME/lib/guava-12.0.1.jar,\
$HBASE_HOME/lib/hbase-protocol*.jar,\
$HBASE_HOME/lib/htrace-core-3.1.0-incubating.jar,\
$HBASE_HOME/lib/metrics-core-2.2.0.jar \
--files $HBASE_HOME/conf/hbase-site.xml CerealHumidity.py 