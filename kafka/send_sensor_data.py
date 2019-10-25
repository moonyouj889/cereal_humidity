#    Copyright 2019 Julie Jung <moonyouj889@gmail.com>

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


# zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
import sys
import os
import argparse
import logging
import gzip
import time
import datetime
from kafka import SimpleClient, KafkaProducer
from pathlib import Path  # python3 only

SENSOR_TOPIC = 'sensor'
LAB_TOPIC = 'lab'
AVG_TOPIC = 'averages'
INPUT = ('../data/processed_data/' +
         'chrono-comma-sep-food-beverage-drying-process.csv.gz')
# KAFKA_LISTENER = os.getenv("KAFKA_LISTENER")
KAFKA_LISTENER = 'localhost:9092'


def peek_timestamp(ifp):
    # peek ahead to next line, get timestamp and go back
    pos = ifp.tell()
    line = ifp.readline()
    ifp.seek(pos)
    return get_timestamp(line.decode('utf-8'))


def get_timestamp(row):
    # convert from bytes to str and look at first field of row
    # print("PRINTINGROW: {}".format(row))
    # row = row.decode('utf-8')
    # print("PRINTINGROW: {}".format(row))
    timestamp = row.split(',')[0]
    # convert from isoformat to regular datetime
    return datetime.datetime.fromisoformat(timestamp)


def publish(publisher, topic, messages):
    # publish to a topic in pubsub
    num_of_events = len(messages)
    if num_of_events > 0:
        logging.info('Publishing {} row(s) starting on {}'.format(
            num_of_events, get_timestamp(messages[0].decode('utf-8'))))
        for message in messages:
            publisher.send(topic, message)


def splitRow(row, columnNames):
    timestamp = row[0]  # timestamp shared between lab and sensor data
    labResult = row[1]  # will be empty str if no result reported
    sensorValues = row[2:]
    sensorMessage = (','.join([timestamp] + sensorValues)).encode('utf-8')
    labMessage = None if labResult == '' else (','.join([timestamp, labResult]).encode('utf-8'))
    return (sensorMessage, labMessage)


def simulate(sensorTopic, labTopic, sensorData,
             firstObsTime, programStart, speedFactor, columnNames):
    # compute how many seconds you have to wait
    # to match the data's time increments to actual time passing
    def get_seconds_to_match(event_time):
        # how many seconds passed since start of simulation
        real_delta_t = (datetime.datetime.utcnow() - programStart).total_seconds()
        # if speedFactor = 1, you simulate at the same rate as data's delta t
        sim_delta_t = (event_time - firstObsTime).total_seconds() / speedFactor
        seconds_to_match = sim_delta_t - real_delta_t
        return (seconds_to_match, real_delta_t, sim_delta_t)

    columnNames = columnNames.decode('utf-8')
    msgsToSensor = []  # collects rows of sensor data
    msgsToLab = []  # collects rows of lab data

    for row in sensorData:
        # print("PRINTINGROWATSIM: {}".format(row))
        parsedRow = row.decode('utf-8')
        # print("PRINTINGROWATSIMAFTERCONVERT: {}".format(parsedRow))
        # retrieve timestamp of current row
        # event_time = get_timestamp(row)
        parsedRow = parsedRow.split(',')
        try:
            event_time = datetime.datetime.fromisoformat(parsedRow[0])
        except ValueError as e:
            logging.error(e)
            logging.error('here is the row: {}'.format(parsedRow))  
        # Check for empty data. And discard them.
        # This is hardcoded since I know that the original data
        # has a bunch of empty rows.
        # logging.error("HERE IS THE PARSED ROW: {}".format(parsedRow))
        # logging.error("HERE IS THE LAST VAL OF ROW: {}".format(parsedRow[-1]))
        if parsedRow[3] == "":
            # Skip to the next row
            continue

        # if there should be waiting for more than 1 second,
        # ( < 1 sec too short to care to delay)
        timeCalc = get_seconds_to_match(event_time)
        if timeCalc[0] > 0:
            # publish the accumulated messages
            if msgsToSensor:
                publish(publisher, sensorTopic, msgsToSensor)
                msgsToSensor = []  # empty out messages to send
            if msgsToLab:
                publish(publisher, labTopic, msgsToLab)
                msgsToLab = []  # empty out messages to send

            # recompute wait time, since publishing takes time
            seconds_to_match = get_seconds_to_match(event_time)[0]
            # despite waiting, if there still are seconds to match,
            if seconds_to_match > 0:
                logging.info('Sleeping {} second(s)'.format(seconds_to_match))
                # wait for real time to match the event time
                time.sleep(seconds_to_match)
        if msgsToSensor != []: logging.error('NO SECONDS TO MATCH, REAL: {}, SIM: {}, eventtime {}, firstObsTime: {}'.format(timeCalc[1], timeCalc[2], event_time, firstObsTime))
        # split from original table to two tables
        sensorMessage, labMessage = splitRow(parsedRow, columnNames)
        if sensorMessage:
            msgsToSensor.append(sensorMessage)
        if labMessage:
            msgsToLab.append(labMessage)

    # left-over records; notify again
    if msgsToSensor:
        publish(publisher, sensorTopic, msgsToSensor)
        msgsToSensor = []  # empty out messages to send
    if msgsToLab:
        publish(publisher, labTopic, msgsToLab)
        msgsToLab = []  # empty out messages to send


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Send oven sensor data and manually recorded ' +
                    'humidity data of the cereal.')
    parser.add_argument(
        '--speedFactor', required=True, type=int,
        help=('Factor to speed up the simultion by. ' +
              'Ex) 60->1 hour worth of data gets sent in 1 minute'))
    args = parser.parse_args()

    # logging status of data simulation
    logging.basicConfig(
        format='%(levelname)s: %(message)s', level=logging.INFO)

    # establish Kafka client
    kafkaClient = SimpleClient(KAFKA_LISTENER)
    # print('KAFKACLIENT FORMAT:{}'.format(type(kafkaClient.client_id)))
    publisher = KafkaProducer(bootstrap_servers=KAFKA_LISTENER,
                              client_id=kafkaClient.client_id.decode('utf-8'))

    # check if topic 1 and topic 2 was created, else create the topic
    if SENSOR_TOPIC in kafkaClient.topics:
        logging.info('Found kafka topic {}'.format(SENSOR_TOPIC))
    else:
        kafkaClient.ensure_topic_exists(SENSOR_TOPIC)
        logging.info('Creating kafka topic {}'.format(SENSOR_TOPIC))
    if LAB_TOPIC in kafkaClient.topics:
        logging.info('Found kafka topic {}'.format(LAB_TOPIC))
    else:
        kafkaClient.ensure_topic_exists(LAB_TOPIC)
        logging.info('Creating kafka topic {}'.format(LAB_TOPIC))
    if AVG_TOPIC in kafkaClient.topics:
        logging.info('Found kafka topic {}'.format(AVG_TOPIC))
    else:
        kafkaClient.ensure_topic_exists(AVG_TOPIC)
        logging.info('Creating kafka topic {}'.format(AVG_TOPIC))
    logging.info('Here are the avilable topics: {}'.format(kafkaClient.topics))

    # notify about each line in the input file
    programStartTime = datetime.datetime.utcnow()
    with gzip.open(INPUT, 'rb') as sensor_data:
        columnNames = sensor_data.readline()
        firstObsTime = peek_timestamp(sensor_data)
        logging.info('Sending sensor data from {}'.format(firstObsTime))
        simulate(SENSOR_TOPIC, LAB_TOPIC, sensor_data, firstObsTime,
                 programStartTime, args.speedFactor, columnNames)