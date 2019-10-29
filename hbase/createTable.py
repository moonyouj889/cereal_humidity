
import happybase
import ast
import json 

'''
make sure that the thrift server is running to connect
if not, use the command: 
   $ sudo bin/hbase-daemon.sh start thrift -p 9090
while in $HBASE_HOME directory
'''

connection = happybase.Connection('localhost', port=9090)

connection.open()
print (connection.tables())

master_table = connection.table('test')
# connection.disable_table('dow_stats')
# connection.delete_table('dow_stats')

try: 
   connection.create_table(
      'dow_stats',
      {'c':dict()}
   )
except:
   print("dow_stats table already created")

statsTbl = connection.table('dow_stats')

for key, data in master_table.scan():
    metrics = {'pickups': data['c:pickups'], 'dropoffs': data['c:dropoffs'], 'occ': data['c:occ'], 'dist': data['c:distance']}
    new_key = key.split('-')[0]
    statsTbl.put(new_key, {'c:' + data['c:hour']: json.dumps(metrics)}) 

# # Scan the aggregate table to store the sums as the last column for fast retrieval
# for key, val in statsTbl.scan():
#     val_ovr_hrs = [ast.literal_eval(val[col]) for col in val]
#     TotPickups = sum(float(item['pickups']) for item in val_ovr_hrs)
#     TotDrops = sum(float(item['dropoffs']) for item in val_ovr_hrs)
#     AvgOcc = sum(float(item['occ']) for item in val_ovr_hrs)/float(len(val_ovr_hrs))
#     AvgDist = sum(float(item['dist']) for item in val_ovr_hrs)/float(len(val_ovr_hrs))    
#     metrics = {'TPickups':TotPickups, 'TDropoffs':TotDrops, 'Avocc':AvgOcc, 'Avdist':AvgDist}
#     statsTbl.put(key, {'c:' + 'Totals':json.dumps(metrics)}) 
