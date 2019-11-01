# Copyright 2019 Julie Jung <moonyouj889@gmail.com>

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, lit
import os
import argparse
import datetime
from dateutil.parser import parse
import subprocess

'''
Make sure that the avro files are in HDFS prior to running the application.

running script:
spark-submit --master yarn --packages org.apache.spark:spark-avro_2.11:2.4.4 CerealHumidity.py 

due to issue with spark2.4 compatibility with shc, build from master locally, then use jar instead.
Make sure to put hbase-client version 3.0.0-SNAPTSHOT in pom before building

spark-submit --verbose --master yarn --packages org.apache.spark:spark-avro_2.11:2.4.4 \
--jars /usr/local/shc/core/target/shc-core-1.1.3-2.4-s_2.11.jar,\
$HBASE_HOME/lib/hbase-client*.jar,\
$HBASE_HOME/lib/hbase-common*.jar,\
$HBASE_HOME/lib/hbase-server*.jar,\
$HBASE_HOME/lib/guava-12.0.1.jar,\
$HBASE_HOME/lib/hbase-protocol*.jar,\
$HBASE_HOME/lib/htrace-core-3.1.0-incubating.jar,\
$HBASE_HOME/lib/metrics-core-2.2.0.jar \
--files $HBASE_HOME/conf/hbase-site.xml MultipleBatchAnalysis.py 
'''

FILEDIR = "hdfs://localhost:9000/user/hadoop/cereal/data/"
HBASE_CATALOG = '{"table":{"namespace":"default", "name":"batchHumidityAnalysis"},\
                "rowkey":"key",\
                "columns":{\
                    "key":{"cf":"rowkey", "col":"key", "type":"string"},\
                    "startTimestamp":{"cf":"METER", "col":"startTimestamp", "type":"string"},\
                    "endTimestamp":{"cf":"METER", "col":"endTimestamp", "type":"string"},\
                    "productHumidity":{"cf":"METER", "col":"productHumidity", "type":"double"},\
                    "avg(waterFlowProcess)":{"cf":"METER", "col":"avgWaterFlowProcess", "type":"double"},\
                    "avg(intensityFanProcess)":{"cf":"METER", "col":"avgIntensityFanProcess", "type":"double"},\
                    "avg(waterTemperatureProcess)":{"cf":"METER", "col":"avgWaterTemperatureProcess", "type":"double"},\
                    "avg(temperatureProcess1)":{"cf":"METER", "col":"avgTemperatureProcess1", "type":"double"},\
                    "avg(temperatureProcess2)":{"cf":"METER", "col":"avgTemperatureProcess2", "type":"double"},\
                    "avg(inputTemperatureProduct)":{"cf":"METER", "col":"avginputTemperatureProduct", "type":"double"}\
                    }\
                }'

def main(spark, avroFilePath):
    df = spark.read.format("avro").load(avroFilePath)
    orderedAvro = df.orderBy(df.timestamp)
    partitionMarkers = df.where(df.productHumidity.isNotNull()).select(df.timestamp).orderBy(df.timestamp).collect()
    fileDate, startDate = "", ""
    dailyAvgsTable = None

    for date in partitionMarkers:
        endDate = date.timestamp
        print("START: {}\tEND: {}".format(startDate, endDate))
        partitionedTable = orderedAvro.filter((df.timestamp > startDate) & (df.timestamp <= endDate))
        avgsDF = partitionedTable.agg({
                    "inputTemperatureProduct": "avg",
                    "waterFlowProcess": "avg",
                    "intensityFanProcess": "avg",
                    "waterTemperatureProcess": "avg",
                    "temperatureProcess1": "avg",
                    "temperatureProcess2": "avg"})
        startTimeVal = partitionedTable.agg({"timestamp": "min"}).collect()[0]["min(timestamp)"]
        endTimeVal = partitionedTable.agg({"timestamp": "max"}).collect()[0]["max(timestamp)"]
        productHumidityVal = partitionedTable.agg({"productHumidity": "max"}).collect()[0]["max(productHumidity)"]

        # add column for rowkey val of HBase
        # in theory, when multiple factories and multiple ovens exist, this is a useful format
        # [factory_id]#[oven_id]#[data_date]#[startTime]#[endTime]
        # e.g. 001#001#20140521#2021#2115
        if fileDate == "": fileDate = parse(endDate) # extract the date of data just once
        fileDateYYYYMMDD =  "{}{:02}{:02}".format(fileDate.year, fileDate.month, fileDate.day)
        startTime, endTime = parse(startTimeVal), parse(endTimeVal)
        startTimeHHMM = "{:02}{:02}".format(startTime.hour, startTime.minute) # HHMM since data every minute
        endTimeHHMM = "{:02}{:02}".format(endTime.hour, endTime.minute)
        key = "001#001#{}#{}#{}".format(fileDateYYYYMMDD, startTimeHHMM, endTimeHHMM)
        newAvgsRow = avgsDF.withColumn("startTimestamp", lit(startTimeVal)) \
                        .withColumn("endTimestamp", lit(endTimeVal)) \
                        .withColumn("productHumidity", lit(productHumidityVal)) \
                        .withColumn("key", lit(key))
                        
        if dailyAvgsTable:
            dailyAvgsTable = dailyAvgsTable.union(newAvgsRow)
        else:
            dailyAvgsTable = newAvgsRow
        startDate = date.timestamp

    # print("===================================================================")
    # # print(dailyAvgsTable.schema.names)
    # for row in dailyAvgsTable.collect():
    #     print(row)

    #Write to HBase
    dailyAvgsTable.write\
        .options(catalog=HBASE_CATALOG)\
        .format("org.apache.spark.sql.execution.datasources.hbase")\
        .save()

    for row in dailyAvgsTable.select(dailyAvgsTable.key).collect():
        print("wrote to HBase in table \"batchHumidityAnalysis\" row: {}".format(row.key))

if __name__ == '__main__':
    spark = SparkSession.builder.appName("CerealHumidity").config("spark.hadoop.validateOutputSpecs", False).getOrCreate()
    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    shellCommand = "export HADOOP_USER_NAME=root && $HADOOP_HOME/bin/hdfs dfs -ls {}*.avro |  awk '{{print $8}}'".format(FILEDIR)
    print(shellCommand)
    try:
        p = subprocess.Popen(shellCommand, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for filepath in p.stdout.readlines():
            print(filepath.decode('utf-8').rstrip('\n'))
            main(spark, filepath.decode('utf-8').rstrip('\n'))

        # Check if batch analysis performed successfully
        batchHumidityAnalysisDf = spark.read \
            .options(catalog=HBASE_CATALOG) \
            .format('org.apache.spark.sql.execution.datasources.hbase') \
            .load()

        print("============================================================")
        batchHumidityAnalysisDf.show()
    except:
        ("could not read from p")

    spark.stop()