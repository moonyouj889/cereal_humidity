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

import csv
from datetime import datetime
import gzip
import os
import shutil
import logging
import json


DATA_FOLDER = './data/processed_data/'
INPUT = 'food-beverage-drying-process.csv'
OUTPUT = DATA_FOLDER + 'chrono-comma-sep-' + INPUT
FIELD_CLEANED_OUTPUT = DATA_FOLDER + 'data-to-pubsub-' + INPUT
SCHEMA_OUTPUT = DATA_FOLDER + 'bq_schemas.txt'
COMMA_SEP_FILE = DATA_FOLDER + 'comma-sep-data.csv'
FIELDNAMES_OUTPUT = DATA_FOLDER + 'fieldNames.txt'

# takes in header from original csv and prints them for user to visualize
# also writes the json of schema to be saved in a separate file
# to be used for Big Query for the ease of analysts querying
'''
load schema format:
{'fields': [
    {'name':'timestamp',
     'type':'TIMESTAMP',
     'mode':'REQUIRED'
    }, {...}
]}
where everything except the timestamps are int type and nullable
'''


def translate_fieldNames(fieldNames):
    buildingNum = ''
    # initialize schemas and field names since timestamp exists in all
    schemas = [{}, {}, {}, {}, {}, {}, {}, {}]
    for schema in schemas:
        schema['fields'] = []
        schema['fields'].append({'name': 'timestamp',
                                 'type': 'TIMESTAMP',
                                 'mode': 'REQUIRED'})
        schema['fields'].append({'name': 'building_id',
                                 'type': 'INTEGER',
                                 'mode': 'REQUIRED'})
    field_names = ['timestamp']

    for field in fieldNames:
        # if field is "Timestamp", continue to nxt iteration
        if field == "Timestamp":
            continue

        if buildingNum != field[0] and buildingNum != '':
            logging.info('')
        buildingNum = int(field[0])

        meterTypeIndicator = field[2]
        schema = schemas[buildingNum-1]
        # in case new fields get added, add them manually in dataflow file
        if meterTypeIndicator == 'M' or meterTypeIndicator == 'S':
            # handle general meter
            fieldType = 'FLOAT'
            fieldMode = 'NULLABLE'
            if meterTypeIndicator == 'M':
                meterType = 'Gen'
                fieldName = '{}'.format(meterType)
            # handle sub meter
            else:
                meterType = 'Sub'
                idNum = ''
                idStart = 14
                while field[idStart].isnumeric():
                    idNum += field[idStart]
                    idStart += 1
                fieldName = '{}_{}'.format(meterType, idNum)

            schema['fields'].append({'name': fieldName,
                                     'type': fieldType,
                                     'mode': fieldMode})
        field_names.append(fieldName)
        logging.info('{} --> {}'.format(field, fieldName))

    logging.info('')
    return (schemas, field_names)


if __name__ == '__main__':
    logging.basicConfig(
        format='%(message)s', level=logging.INFO)

    try:
        shutil.rmtree(DATA_FOLDER)
        logging.info("INFO: Old processed csv found and removed.")
        os.mkdir(DATA_FOLDER)
        logging.info(
            "INFO: processed_data directory recreated.")
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

    logging.info("Continuing csv processing...")
    logging.info("\n_______FIELD NAMES_______")

    logging.info('''
    datetime
    productHumidity
    processIsOn
    inputTemperatureProduct
    waterFlowProcess
    intensityFanProcess
    waterTemperatureProcess
    temperatureProcess1
    temperatureProcess2
    ''')

    # sort the order of ther rows by timestamps
    with open('./data/' + INPUT, 'r') as jumbled_data,  \
            open(COMMA_SEP_FILE, 'w') as csv_data:
        jumbled_rows = csv.reader(jumbled_data, delimiter=';')
        jumbled_rows_header = next(jumbled_rows)
        # bq_schemas, fieldNames = translate_fieldNames(jumbled_rows_header)
        # jumbled_rows = csv.DictReader(jumbled_data, delimiter=';')
        # jumbled_rows.fieldnames = fieldNames

        csv_rows = csv.writer(csv_data, delimiter=',')
        csv_rows.writerow(jumbled_rows_header)
        csv_rows.writerows(jumbled_rows)
        # chrono = jumbled_rows

    # with open(SCHEMA_OUTPUT, 'w') as schema_file:
    #     json.dump(bq_schemas, schema_file)
    #     logging.info( 
    #         "INFO: bq_schemas.txt created.")

    with open(COMMA_SEP_FILE, 'r') as csv_reader, open(OUTPUT, 'w') as chronological_data:
        jumbled_rows = csv.reader(csv_reader, delimiter=',')
        jumbled_rows_header = next(jumbled_rows)
        chrono_data = sorted(jumbled_rows, key=lambda row: (row[0]))
        chronological_rows = csv.writer(chronological_data, delimiter=',')
        chronological_rows.writerow(jumbled_rows_header)
        chronological_rows.writerows(chrono_data)
        # chronological_rows.writerows(jumbled_rows)
        logging.info("INFO: chronological data created.")

    with open(OUTPUT, 'rb') as f_in, gzip.open(OUTPUT + '.gz', 'wb') as f_out:
        f_out.writelines(f_in)
        logging.info(
            "INFO: gzipFile of chronological data created.")
