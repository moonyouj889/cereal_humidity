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
# import spark
import os
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("CerealHumidity").getOrCreate()
# logData = spark.read.text(logFile).cache()

'''
run by typing: python SimpleApp.py
Since pip installed
'''

path = './avroFiles'

'''
# How to Write to HBase
def catalog = '{
        "table":{"namespace":"default", "name":"table1"},\
        "rowkey":"key",\
        "columns":{\
          "col0":{"cf":"rowkey", "col":"key", "type":"string"},\
          "col1":{"cf":"cf1", "col":"col1", "type":"boolean"},\
          "col2":{"cf":"cf1", "col":"col2", "type":"double"},\
          "col3":{"cf":"cf1", "col":"col3", "type":"float"},\
          "col4":{"cf":"cf1", "col":"col4", "type":"int"},\
          "col5":{"cf":"cf2", "col":"col5", "type":"bigint"},\
          "col6":{"cf":"cf2", "col":"col6", "type":"smallint"},\
          "col7":{"cf":"cf2", "col":"col7", "type":"string"},\
          "col8":{"cf":"cf2", "col":"col8", "type":"tinyint"}\
        }\
      }'
df.write\
    .options(catalog=catalog)\
    .format("org.apache.spark.sql.execution.datasources.hbase")\
    .save()
'''
'''
# for filename in os.listdir(path):
    # df = spark.read.format("avro").load(filename)
    # df.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")
    # logFile = "/Users/juliejung/Documents/deProjects/cereal_humidity/sparkTest.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
'''
df = spark.read.format("avro").load("file:///Users/juliejung/Documents/deProjects/cereal_humidity/py-spark/avroFiles/kafkamsgs2019-10-26T13:06:00.000Z-2019-10-26T13:12:00.000Z-pane-0-last-00000-of-00001.avro")
avroFile = df.collect()
for row in avroFile:
    print(row)
'''
from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local[2]"))

import re

def parse_article(line):
    try:
        article_id, text = line.rstrip().split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []
    
def pairs(words):
    out = []
    for w1, w2 in zip(words, words[1:]):
        out.append((w1.lower() + "_" + w2.lower(), 1))
    return out
    
result = (sc.textFile("/data/wiki/en_articles_part/articles-part", 16)
        .map(parse_article)
        .flatMap(pairs)
        .reduceByKey(lambda x,y : x+y)
        .filter(lambda value: value[0][:9] == "narodnaya")
       ).collect()

for key, count in result:
    print("%s\t%d" % (key, count))
'''