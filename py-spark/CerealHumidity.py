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


'''
running script:
spark-submit --master yarn --packages org.apache.spark:spark-avro_2.11:2.4.4 CerealHumidity.py 
'''


spark = SparkSession.builder.appName("CerealHumidity").getOrCreate()
log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

# logData = spark.read.text(logFile).cache()

FILEDIR = 'hdfs://localhost:9000/user/hadoop/cereal/data/'

parser = argparse.ArgumentParser()
parser.add_argument("--avroFile", help="avro file name to perform batch analysis on", type=str,
    default="kafkamsgs2019-10-26T13_06_00.000Z-2019-10-26T13_12_00.000Z-pane-0-last-00000-of-00001.avro")
args = parser.parse_args()

df = spark.read.format("avro").load(FILEDIR + args.avroFile)
orderedAvro = df.orderBy(df.timestamp)
partitionMarkers = df.where(df.productHumidity.isNotNull()).select(df.timestamp).orderBy(df.timestamp).collect()
startDate = ""
partitionedTables = []

for date in partitionMarkers:
    print("START: {}\tEND: {}".format(startDate, date.timestamp))
    partitionedTables.append(orderedAvro.filter((df.timestamp > startDate) & (df.timestamp <= date.timestamp)))
    startDate = date.timestamp

print("number of partitioned tables: {}".format(len(partitionedTables)))
print("-----------------------------------------------------------")

for table in partitionedTables:
    avgsDF = table.agg({"inputTemperatureProduct": "avg",
                     "waterFlowProcess": "avg",
                     "intensityFanProcess": "avg",
                     "waterTemperatureProcess": "avg",
                     "temperatureProcess1": "avg",
                     "temperatureProcess2": "avg"}) \
                # .withColumn("startTimestamp", table.select(min('age').over())) \
                # .withColumn("endTimestamp", table.max("timestamp").over())
    startTimeVal = table.agg({"timestamp": "min"}).collect()[0]["min(timestamp)"]
    endTimeVal = table.agg({"timestamp": "max"}).collect()[0]["max(timestamp)"]
    productHumidityVal = table.agg({"productHumidity": "max"}).collect()[0]["max(productHumidity)"]
    finalDF = avgsDF.withColumn("startTimestamp", lit(startTimeVal)) \
                    .withColumn("endTimestamp", lit(endTimeVal)) \
                    .withColumn("productHumidity", lit(productHumidityVal))
    print(finalDF.collect())
    print("=========================================================")



'''
Row(timestamp='2014-05-21T04:58:00-04:00', 
    productHumidity=None, 
    processOn=False, 
    inputTemperatureProduct=44.29999923706055, 
    waterFlowProcess=0.0, 
    intensityFanProcess=0.0, 
    waterTemperatureProcess=36.54999923706055, 
    temperatureProcess1=20.5, 
    temperatureProcess2=22.149999618530273)

startTimestamp
endTimestamp
productHumidity
avgInputTemperatureProduct
avgWaterFlowProcess
avgIntensityFanProcess
avgWatertemperatureProcess
avgTemperatureProcess1
avgTemperatureProcess2
'''

#Write to HBase
# def catalog = '{
#         "table":{"namespace":"default", "name":"table1"},\
#         "rowkey":"key",\
#         "columns":{\
#           "col0":{"cf":"rowkey", "col":"key", "type":"string"},\
#           "col1":{"cf":"cf1", "col":"col1", "type":"boolean"},\
#           "col2":{"cf":"cf1", "col":"col2", "type":"double"},\
#           "col3":{"cf":"cf1", "col":"col3", "type":"float"},\
#           "col4":{"cf":"cf1", "col":"col4", "type":"int"},\
#           "col5":{"cf":"cf2", "col":"col5", "type":"bigint"},\
#           "col6":{"cf":"cf2", "col":"col6", "type":"smallint"},\
#           "col7":{"cf":"cf2", "col":"col7", "type":"string"},\
#           "col8":{"cf":"cf2", "col":"col8", "type":"tinyint"}\
#         }\
#       }'
# df.write\
#     .options(catalog=catalog)\
#     .format("org.apache.spark.sql.execution.datasources.hbase")\
#     .save()


spark.stop()