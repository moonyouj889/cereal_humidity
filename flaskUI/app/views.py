from app import app
import flask
from flask import jsonify, render_template, Response, request
from kafka import KafkaClient, KafkaConsumer
from werkzeug.exceptions import HTTPException, NotFound, abort
import json
import datetime
import time
import happybase
from struct import unpack

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DevelopmentConfig

HOSTS = DevelopmentConfig.kafka["hosts"]
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
BATCH_ANALYSIS_TABLE_NAME = 'batchHumidityAnalysis'
RUNNING_AVG_TABLE_NAME = 'runningAvgAnalysis'
SIM_DAY_IN_MIN = 12 # For simulation, when speedFactor = 240, 1 day = 6 minutes

@app.route('/')
@app.route('/index', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/current', methods=['GET'])
def current():
    return render_template('index.html')


@app.route('/batch', methods=['GET'])
def batch():
    return render_template('pages/batch.html')


def get_kafka_client():
    if not hasattr(flask.g, "kafka_client"):
        flask.g.kafka_client = KafkaClient(HOSTS)
    return flask.g.kafka_client


def events(topic):
    for msg in KafkaConsumer(topic):
        yield 'data: {}\n\n'.format(msg.value.decode('utf-8'))


@app.route('/data/sensor', methods=['GET'])
def get_sensor_messages():
    client = get_kafka_client()
    return Response(events('sensor'), mimetype="text/event-stream")


@app.route('/data/averages', methods=['GET'])
def get_averages_messages():
    client = get_kafka_client()
    return Response(events('averages'), mimetype="text/event-stream")

# For simulation, where speedFactor = 480, so 24 hours = 3 minutes
@app.route('/data/past24sim', methods=['GET'])
def get_averages_past24sim():
    # Streaming avgs are based on actual time, 
    # and saved in hbase with rowkey based on actual time.
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    conn.open()
    # yyyyMMddHHmm
    date = datetime.datetime.now()
    pastSimDay = (date - datetime.timedelta(minutes=SIM_DAY_IN_MIN)).strftime("%Y%m%d%H%M")
    simToday = date.strftime("%Y%m%d%H%M")
    try:
        table = happybase.Table(RUNNING_AVG_TABLE_NAME, conn)
        row_start = '001#001#{}'.format(pastSimDay)
        row_start_bytes = row_start.encode('utf-8')
        row_end = '001#001#{}'.format(simToday)
        row_end_bytes = row_end.encode('utf-8')
        rows = []
        
        for key, data in table.scan(row_start=row_start_bytes, row_stop=row_end_bytes):
            keyStr = key.decode('utf-8')
            rowDataDict = {}
            for columnName in data:
                column = columnName.decode('utf-8')
                # Java Bytes class converts Double to IEEE-754
                # String is converted by utf-8
                n = unpack(b'>d', data[columnName])
                val = round(n[0], 2)
                rowDataDict[column] = val
            rows.append((keyStr, rowDataDict))
        app.logger.info('Retrieved data from HBase succesfully')
        return jsonify(items=rows)
    except:
        errorMsg = "Table {} doesn't have row {}. Check with the hbase shell that you're retrieving the correct data.".format(BATCH_ANALYSIS_TABLE_NAME, pastSimDay)
        app.logger.error(errorMsg)
        return errorMsg
    finally:
        conn.close()

# For real scenario
@app.route('/data/past24', methods=['GET'])
def get_averages_past24():
    # Streaming avgs are based on actual time, 
    # and saved in hbase with rowkey based on actual time.
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    conn.open()
    # yyyyMMddHHmm
    date = datetime.datetime.now()
    past24Hrs = (date - datetime.timedelta(hours=24)).strftime("%Y%m%d%H%M")
    today = date.strftime("%Y%m%d%H%M")
    try:
        table = happybase.Table(RUNNING_AVG_TABLE_NAME, conn)
        row_start = '001#001#{}'.format(past24Hrs)
        row_start_bytes = row_start.encode('utf-8')
        row_end = '001#001#{}'.format(today)
        row_end_bytes = row_end.encode('utf-8')
        rows = []
        
        for key, data in table.scan(row_start=row_start_bytes, row_stop=row_end_bytes):
            keyStr = key.decode('utf-8')
            rowDataDict = {}
            for columnName in data:
                column = columnName.decode('utf-8')
                # Java Bytes class converts Double to IEEE-754
                # String is converted by utf-8
                n = unpack(b'>d', data[columnName])
                val = round(n[0], 2)
                rowDataDict[column] = val
            rows.append((keyStr, rowDataDict))
        app.logger.info('Retrieved data from HBase succesfully')
        return jsonify(items=rows)
    except:
        errorMsg = "Table {} doesn't have row {}. Check with the hbase shell that you're retrieving the correct data.".format(BATCH_ANALYSIS_TABLE_NAME, past24Hrs)
        app.logger.error(errorMsg)
        return errorMsg
    finally:
        conn.close()


@app.route('/data/batch/yesterday/<date>', methods=['GET'])
def get_batch_yesterday(date=None):
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    conn.open()
    app.logger.info(date)
    if not date: 
        date = request.args.get('date')
    datetimeDate = datetime.datetime.strptime(date, "%Y%m%d")
    # return date
    pastWeek = (datetimeDate - datetime.timedelta(days=7)).strftime("%Y%m%d")
    yesterday = datetimeDate.strftime("%Y%m%d")
    # arguments formatted to yyyyMMdd
    row_start = '001#001#{}'.format(pastWeek)
    row_start_bytes = row_start.encode('utf-8')
    row_end = '001#001#{}'.format(yesterday)
    row_end_bytes = row_end.encode('utf-8')
    try:
        table = happybase.Table(BATCH_ANALYSIS_TABLE_NAME, conn)

        rows = []
        for key, data in table.scan(row_start=row_start_bytes, row_stop=row_end_bytes):
            keyStr = key.decode('utf-8')
            rowDataDict = {}
            for columnName in data:
                column = columnName.decode('utf-8')
                try: 
                    # Java Bytes class converts Double to IEEE-754
                    # String is converted by utf-8
                    n = unpack(b'>d', data[columnName])
                    val = round(n[0], 2)
                except:
                    val = data[columnName].decode('utf-8')
                rowDataDict[column] = val
            # pass
            rows.append((keyStr, rowDataDict))
        app.logger.info('Retrieved data from HBase succesfully')
        return jsonify(items=rows)
    except:
        app.logger.error("Table {} doesn't have row {}. Check with the hbase shell that you're retrieving the correct data.".format(BATCH_ANALYSIS_TABLE_NAME, row_start))
        return "ERROR"
    finally:
        conn.close()


# @app.route('/data/batch/pastweek/<pastweek>_<yesterday>', methods=['GET'])
# def get_batch_week(pastweek=None, yesterday=None):
#     conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
#     conn.open()
#     print (conn.tables())
#     try:
#         if not pastweek: 
#             pastweek = request.args.get('pastweek')
#             yesterday = request.args.get('yesterday')
#         table = happybase.Table(BATCH_ANALYSIS_TABLE_NAME, conn)
#         # arguments formatted to yyyyMMdd
#         row_start = '001#001#{}'.format(pastweek)
#         row_start_bytes = row_start.encode('utf-8')
#         row_end = '001#001#{}'.format(yesterday)
#         row_end_bytes = row_end.encode('utf-8')
#         rows = []
#         for key, data in table.scan(row_start=row_start_bytes, row_stop=row_end_bytes):
#             # print(key, data)
#             keyStr = key.decode('utf-8')
#             rowDataDict = {}
#             for columnName in data:
#                 column = columnName.decode('utf-8')
#                 try: 
#                     # Java Bytes class converts Double to IEEE-754
#                     # String is converted by utf-8
#                     n = unpack(b'>d', data[columnName])
#                     val = round(n[0], 2)
#                 except:
#                     val = data[columnName].decode('utf-8')
#                 rowDataDict[column] = val
#             # pass
#             rows.append((keyStr,rowDataDict))
#         app.logger.info('Retrieved data from HBase succesfully')
#         return jsonify(items=rows)
#     except:
#         print("No such table/row exists. Check with the hbase shell that you're retrieving the correct data.")
#     finally:
#         conn.close()
        