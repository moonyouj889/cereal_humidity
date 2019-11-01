import happybase
from struct import unpack

BATCH_ANALYSIS_TABLE_NAME = 'batchHumidityAnalysis'

def get_batch_yesterday(date="20140523"):
    conn = happybase.Connection('localhost', port=9090)
    conn.open()
    try:
        rows = []
        table = happybase.Table(BATCH_ANALYSIS_TABLE_NAME, conn)
        # date_yesterday formatted to yyyyMMdd
        row_prefix = '001#001#{}'.format(date)
        row_prefix_bytes = row_prefix.encode('utf-8')
        print(row_prefix_bytes)
        
        for key, data in table.scan(row_prefix=row_prefix_bytes):
            # rows.append((key, data))
            rowDataDict = {}
            for columnName in data:
              column = columnName.decode('utf-8')
              try: 
                # Java Bytes class converts Double to IEEE-754
                # String is converted by utf-8
                n = unpack(b'>d', data[columnName])
                # print(format(n[0], '.18f'))
                val = round(n[0], 2)
              except:
                val = data[columnName].decode('utf-8')
              rowDataDict[column] = val
            # pass
            rows.append(rowDataDict)
        return rows
    except:
        print("No such table/row exists. Check with the hbase shell that you're retrieving the correct data.")
        # app.logger.error("Table {} doesn't have row {}. Check with the hbase shell that you're retrieving the correct data.".format(BATCH_ANALYSIS_TABLE_NAME, row_prefix))
    finally:
        conn.close()

def get_batch_week(pastweek, yesterday):
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
    conn.open()
    print (conn.tables())
    try:
        table = happybase.Table(BATCH_ANALYSIS_TABLE_NAME, conn)
        # arguments formatted to yyyyMMdd
        row_start = '001#001#{}'.format(pastweek)
        row_start_bytes = row_start.encode('utf-8')
        row_end = '001#001#{}'.format(yesterday)
        row_end_bytes = row_end.encode('utf-8')
        rows = {}
        for key, data in table.scan(row_start=row_start_bytes, row_stop=row_end_bytes):
            # print(key, data)
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
            rows[keyStr] = rowDataDict
        app.logger.info('Retrieved data from HBase succesfully')
        return jsonify(rows)
    except:
        print("No such table/row exists. Check with the hbase shell that you're retrieving the correct data.")
    finally:
        conn.close()
        

if __name__ == '__main__':
  print(get_batch_yesterday())
  print(get_batch_week())