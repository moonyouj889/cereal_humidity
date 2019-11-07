# make sure to fix the .properties file so the server isn't localhost:9092 for production
KAFKA_HOME=/usr/local/kafka

# $KAFKA_HOME/bin/zookeeper-server-start.sh /usr/local/etc/kafka/zookeeper.properties & $KAFKA_HOME/kafka-server-start.sh /usr/local/etc/kafka/server.properties
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties & $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties