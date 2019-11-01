export HADOOP_USER_NAME=root
export HADOOP_DIR=hdfs://localhost:9000/user/hadoop/cereal/data/

# rename the files so no ":" is in the filenames
python ./cleanMoveAvro.py

# for all avro files collected from beam pipeline,
for i in /home/julie/avroFiles/*.avro; do
  # move all avro files to hdfs
  $HADOOP_HOME/bin/hdfs dfs -put ${i} ${HADOOP_DIR}
  echo "moved ${i} to ${HADOOP_DIR}"
done

# confirm whether the files were copied to hdfs correctly
$HADOOP_HOME/bin/hdfs dfs -ls /user/hadoop/cereal/data

echo "script complete"