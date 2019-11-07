# make sure to fix the .properties file so the server isn't localhost:9092 for production
if [ -z "$1" ]; then 
    echo "No topic was given. Make sure to provide the topic name at the end."
    exit 1
else
    echo "Instantiating consumer for topic: $1"
    KAFKA_HOME=/usr/local/kafka
    KAFKA_SERVER=localhost:9092
    TOPIC_NAME=$1

    $KAFKA_HOME/bin/kafka-console-consumer.sh \
    --bootstrap-server $KAFKA_SERVER \
    --topic $TOPIC_NAME \
    --from-beginning
fi