ZOOKEPER_HOST=localhost
KAFKA_HOME=/usr/local/kafka

# before running, purge the topic from previous run
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEPER_HOST:2181 --delete --topic sensor
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEPER_HOST:2181 --delete --topic lab

# running for testing
python ./send_sensor_data.py --speedFactor 120

# running for real time
# python ./send_sensor_data.py --speedFactor 1
