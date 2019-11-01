ZOOKEPER_HOST=localhost
KAFKA_HOME=/usr/local/kafka

# before running, purge the topic from previous run
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEPER_HOST:2181 --delete --topic sensor
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEPER_HOST:2181 --delete --topic lab

# running for testing
# speedFactor = 240 = 60 * 4 => one minute in real time is 0.25 sec
python ./send_sensor_data.py --speedFactor 240

# speedFactor = 480 = 60 * 8 => one minute in real time is 0.125 sec
# python ./send_sensor_data.py --speedFactor 480


# running for real time
# python ./send_sensor_data.py --speedFactor 1
