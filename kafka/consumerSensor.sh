# make sure to fix the .properties file so the server isn't localhost:9092 for production
KAFKA_HOME=/usr/local/kafka
TOPIC_NAME=$1

$KAFKA_HOME/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic $TOPIC_NAME \
--from-beginning