# Cereal Factory Product Humidity Monitoring

## Introduction

This is a data engineering project based on the Hadoop Ecosystem, specifically utilizing Kafka, Apache Beam with Flink runner, Spark and HBase running on top of a Hadoop cluster. The raw data was sourced from [Schneider Electric Exchange](https://shop.exchange.se.com/home), under ["Food & beverage drying process"](https://shop.exchange.se.com/apps/39065/food-beverage-drying-process#!overview). The data was collected to monitor and control the moisture level in breakfast cereal production line. The oven was operated with a collection of sensors that reported measurements every minute while the product humidity data were sampled manually in a lab at irregular times. The data span from 5/21/2014 to 6/11/2014. 

There are three problems that this project aims to solve:

1. Batch load all data collected on a daily basis for data analysts or data scientists to perform analysis in the future.
1. Perform daily batch analysis on sensor data in relations to product humidity measurement.
1. Calculate the running average of all sensor data for real time monitoring.

## Table of Contents

1. [Architecture](#architecture)
1. [Data Simulation](#data-simulation)
1. [Data Pipeline](#data-pipeline)
1. [HBase Schema Design](#hbase-schema-design)
1. [Results](#results)
1. [How To Run Locally](#how-to-run-locally)
1. [Further Improvements](#further-improvements)
1. [Licensing](#licensing)

## Architecture

<p align="center"><img src="/imgs/architecture.png"></img></p>

- Ingestion Layer (Kafka):

  - In a realistic scenario, the sensors to Kafka architecture would look something like this:
       <p align="center"><font size="-1">
    <img src="/imgs/mqttKafka.png" width="90%"></br>
          <i>Source: <a href="https://www.slideshare.net/ConfluentInc/processing-iot-data-from-end-to-end-with-mqtt-and-apache-kafka">"Processing IoT Data from End to End with MQTT and Apache Kafka"</a> by Kai Waehner, Confluent</i></font></p>
  - Since the original data is a historical data of the cereal production line, a python script was used to read the original raw csv file, split the table into two, one for sensor data and other for lab data, and were published to two separate topics, "sensor" and "lab". The python script was intended to simulate a scenario where the sensor data is reported from the sensors at the oven and passed to Kafka through the architecture as explained above, and the lab data is reported manually at the lab through a UI that publishes to Kafka directly.

- Batch Layer (Apache Beam/Flink Runner, HDFS, Apache Spark): The messages published from the ingestion layer were read with the Apache Beam's KafkaIO, then the data was split into two routes. 
  1. The sensor data and lab data were merged into one PCollection and translated into Avro files, creating one file per day. These avro files were saved on HDFS for Apache Spark to ingest. With Spark, the data was split based on timestamps of when the humidity measurements were reported. Then, the average values of sensor data before each measurement and after the last measurement of each were calculated. 
  1. The sensor data and lab data remained separate and were translated to HBase viable format and inserted into the HBase tables for further querying and analyses.

- Speed Layer (Apache Beam/Flink Runner, HBase, Kafka): On top of ingesting the data using KafkaIO with the batch layer, the real time analysis of running average of all meter readings was conducted. The results were both stored in HBase and published to a separate topic, "averages", on Kafka for the Dashboard web interface.

- Serving Layer (Apache HBase): HBase ultimately stores three main tables: "runningAvgAnalysis", "currentConditions", and "batchHumidityAnalysis". The table schema is optimized for quick access for future queries or further analyses. The rowkey design is discussed further in [HBase Schema Design](#hbase-schema-design)

## Data Simulation

### Structure of the Original Data

As seen on the [original csv](./data/food-beverage-drying-process.csv) exported from [Schneider Electric Exchange](https://shop.exchange.se.com/home), the original format of the csv was:

| datetime | productHumidity | processIsOn | inputTemperatureProduct | waterFlowProcess | intensityFanProcess | waterTemperatureProcess| temperatureProcess1 | temperatureProcess2 |
| -------- | ----------- | --------------- | -------------- | --------------- | --- | --------------- | -------------- | ------ |
|2014-06-05T06:01:00-04:00| | 1 | 47.5 | 14.666666666666666 | 50.2 | 38.65 | 29.9 | 60.05 |

The timstamp used the UTC ISO format, productHumidity was empty when the measurement was not taken at that time and was measured in %, processIsOn was represented with 1 for "on" and 0 for "off", all of the temperature values were measured in degrees Celsius, and the unit of measurements for waterFlowProcess and intensityFanProcess were not explained in the source of the file.

### Restructured Raw Data for Simulation

Since the original data was cleaned manually and all of the data were gathered into a single table prior to being published on the online library, I wanted to adjust the schema to something more realistic in a scenario where the sensor data were gathered to a single message by the IoT Gateway, while the lab data that was manually reported was separate from the sensor data. Example schema of sensor data and lab data are as shown:

| datetime                  | processIsOn | inputTemperatureProduct | waterFlowProcess | intensityFanProcess | waterTemperatureProcess| temperatureProcess1 | temperatureProcess2 |
| -------- | ----------- | --------------- | -------------- | --------------- | --- | --------------- | -------------- |
|2014-06-05T06:01:00-04:00| 1 | 47.5 | 14.666666666666666 | 50.2 | 38.65 | 29.9 | 60.05 |

| datetime                  | productHumidity |
| -------------------------- | ----------- |
| 2014-05-30T04:10:00-04:00 | 7.805    |

### SpeedFactor

When running the `send_meter_data.py` to lauch the data publishing simulation to Kafka, the user must provide the `speedFactor`. The `speedFactor` allows the user to quicken the simulation of data. For example, if the user provides the SpeedFactor of 60, one event row of the original data would be sent per second, quickening the simulation time to be 60 times faster than actual time. More accurately, after the change in the schema, one row of the sensor data would be sent to Kafka per seconds, but the time between lab data would vary, depending on the report time. To match the original data time increment, the user must provide the SpeedFactor of 1. In the testing environment, I used the speedFactor of 240, meaning one row of sensor data was published roughly in 0.25 second.

### Simulation Time vs. Actual Time

Even though the data was sent to Kafka in relations to the actual time, the datetime values were kept as the original values, rather than recreated at the time of publishing to Kafka. This was for the development environment so that the "currentConditions" and "batchHumidityAnalysis" tables do not receive new rows for the same original data every time the Beam or Spark application are run for testing. On the other hand, the actual time was used to simulate real time behavior for running averages, so "runningAvgAnalysis" data is saved in relations to the actual time, and streaming data page of the Web Dashboard displays the actual time of the values' update.  


## Data Pipeline

![Beam DAG](./imgs/beamDag.png)

The Beam Pipeline diagram above provides the step by step view of how the data was ingested, aggregated, and loaded, or stream inserted. Starting from the top, the data was read and ingested from Kafka. Then, the pipeline was branched to the stream (on the left), and batch (on the right) processing.

Here is what Apache Flink displayed while running the job:
![Beam DAG](./imgs/flink.png)


### Stream Processing

In the stream processing, the `SlidingWindows` was set, and the general meter readings of each building was aggregated according to the window created to calculate the Mean. The `SlidingWindows` took two requird arguments -- `size` and `period`. The size indicates how wide the window should be in seconds, and the period indicates for how long (in seconds) the aggregation must be recalculated. A sliding window of 60 seconds with period of 30 seconds would look something like this:

 <p align="center"><font size="-1">
    <img src="https://beam.apache.org/images/sliding-time-windows.png" width="70%"></br>
          <i>Source: <a href="https://beam.apache.org/documentation/programming-guide/)*">"Beam Programming Guide"</i></a></font></p>

The `SlidingWindows` on an hourly basis with period of half an hour was specifically chosen instead of a regular `FixedWindows`, because the default `FixedWindows` waits until the end of the window to produce the aggregation results. Since the aim of the stream pipeline was to calculate the real time average of the energy readings, the `SlidingWindows` was chosen.


### Batch Processing

#### Current Conditions

#### Daily Aggregate Analysis

## HBase Schema Design

### "currentConditions" Table
<table>
  <tr>
    <th rowspan="2">Row Key</th>
    <th colspan="4">Column Family: "METER"</th>
  </tr>
  <tr>
<!--     <th>Row Key</th> -->
    <th>COLUMN: 00</th>
    <th>COLUMN: 01</th>
    <th>...</th>
    <th>COLUMN: 59</th>
  </tr>
  <tr>
    <td>001#001#inputTemperatureProduct#20140521#19</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
  </tr>
</table>



### "batchHumidityAnalysis" Table
<table>
  <tr>
    <th rowspan="2">Row Key</th>
    <th colspan="9">Column Family: "METER"</th>
  </tr>
  <tr>
<!--     <th>Row Key</th> -->
    <th>COLUMN: startTimestamp</th>
    <th>COLUMN: endTimestamp</th>
    <th>COLUMN: productHumidity</th>
    <th>COLUMN: avgWaterFlowProcess</th>
    <th>COLUMN: avgIntensityFanProcess</th>
    <th>COLUMN: avgWaterTemperatureProcess</th>
    <th>COLUMN: avgTemperatureProcess1</th>
    <th>COLUMN: avgTemperatureProcess2</th>
    <th>COLUMN: avginputTemperatureProduct</th>
  </tr>
  <tr>
    <td>001#001#20140521#0454#1058</td>
    <td>2014-05-21T04:54:00-04:00</td>
    <td>2014-05-21T10:58:00-04:00</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
  </tr>
</table>

Row Key: "001#001#{}#{}#{}".format(fileDateYYYYMMDD, startTimeHHMM, endTimeHHMM)

### "runningAvgAnalysis" Table
<table>
  <tr>
    <th rowspan="2">Row Key</th>
    <th colspan="7">Column Family: "METER"</th>
  </tr>
  <tr>
<!--     <th>Row Key</th> -->
    <th>COLUMN: processIsOn</th>
    <th>COLUMN: inputTemperatureProduct</th>
    <th>COLUMN: waterFlowProcess</th>
    <th>COLUMN: intensityFanProcess</th>
    <th>COLUMN: waterTemperatureProcess</th>
    <th>COLUMN: temperatureProcess1</th>
    <th>COLUMN: temperatureProcess2</th>
  </tr>
  <tr>
    <td>001#001#201911031349</td>
    <td>true</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
    <td>...</td>
  </tr>
</table>
Row Key: {factory_id}#{oven_id}#{yyyyMMddHHmm} of actual time

## Results

![flask_curr](./imgs/localhost_3000_current.png)
Current/index page displays the streaming data of current sensor data as well as their running averages. The graphs below display the running average values from the past 24 hours of the current time.


![flask_streamDash](./imgs/streamDash.gif) <br/>
Live demo of streaming current sensor data and running averages data onto the Web Dashboard.

![flask_runningAvg24](./imgs/runningAvg24.gif)
Graphical representation of the running averages collected from the past 24 hours from right now.

![flask_batch](./imgs/localhost_3000_batch.png)
Batch page displays the average sensor data between product humidity measurements. Each humidity measurement has corresponding average data values.

![flask_batchAnalysis](./imgs/batchAnalysis.gif)
Graphical representation of the results of the batch analyses performed by Spark for the past week



## How To Run Locally



## Further Improvements

### Improvements for Optimization


### Miscellaneous Improvements
- Apache Phoenix ...
- Security


## Licensing

This project uses the [Apache License 2.0](LICENSE)
