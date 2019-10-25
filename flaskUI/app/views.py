from app import app
import flask
from flask import render_template, Response
from kafka import KafkaClient, KafkaConsumer
from werkzeug.exceptions import HTTPException, NotFound, abort
import json
import datetime
import time

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DevelopmentConfig

HOSTS = DevelopmentConfig.kafka["hosts"]


@app.route('/')
@app.route('/index', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/current')
def maps():
    return render_template('index.html')


@app.route('/batch')
def batch():
    return render_template('pages/batch.html')


def get_kafka_client():
    if not hasattr(flask.g, "kafka_client"):
        flask.g.kafka_client = KafkaClient(HOSTS)
    return flask.g.kafka_client


def events(topic):
    for msg in KafkaConsumer(topic):
        yield 'data: {}\n\n'.format(msg.value.decode('utf-8'))


@app.route('/data/sensor')
def get_sensor_messages():
    client = get_kafka_client()
    return Response(events('sensor'), mimetype="text/event-stream")


@app.route('/data/averages')
def get_averages_messages():
    client = get_kafka_client()
    return Response(events('averages'), mimetype="text/event-stream")
