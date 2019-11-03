// Copyright 2019 Julie Jung <moonyouj889@gmail.com>

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code derived from https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/dataflow/transforms/src/main/java/com/example/CsvToAvro.java

package jj.flinkbeam;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.Collection;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.Mean;

import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import org.joda.time.Duration;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.io.BufferedReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

/*
mvn compile exec:java -Dexec.mainClass=jj.flinkbeam.AvgSensorData \
    -Pflink-runner \
    -Djava.util.logging.config.file=src/main/resources/logging.properties \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/kafkamsgs \
      --flinkMaster=localhost" 

mvn -e -X clean package exec:java -Dexec.mainClass=jj.flinkbeam.AvgSensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/currConditions \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar" \
    -Djava.util.logging.config.file=src/main/resources/logging.properties

mvn clean package exec:java -Dexec.mainClass=jj.flinkbeam.AvgSensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=/home/julie/avroFiles/currConditions \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar" \
    -Djava.util.logging.config.file=src/main/resources/logging.properties

mvn clean package exec:java -Dexec.mainClass=jj.flinkbeam.AvgSensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/SHOULDDELETE \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar" \
    -Djava.util.logging.config.file=src/main/resources/logging.properties

You can monitor the running job by visiting the Flink dashboard at http://localhost:8081
*/
@SuppressWarnings("serial")
public class AvgSensorData {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final String KAFKA_SERVER = "localhost:9092";
  // Used for daily batch load
  // in simulation, 1 min = 0.25 sec, so 24 hrs = 6 min
  // for deployment, use 24*3600
  static final int FIXED_WINDOW_SIZE = 6 * 60;
  // static final int FIXED_WINDOW_SIZE = 3 * 60;
  // Used for running average calculation
  // in simulation, 1 min = 0.25 sec, so use 15 (average calulated every 15 sec,
  // with 7.5 sec overlap)
  // for deployment, use 3600 -- in reality, hour based running avgs w/ 30min
  // overlap
  static final int SLIDING_WINDOW_SIZE = 15;
  // static final int SLIDING_WINDOW_SIZE = 7;
  static final List<String> COLUMNS = Arrays.asList("processIsOn", "inputTemperatureProduct", "waterFlowProcess",
      "intensityFanProcess", "waterTemperatureProcess", "temperatureProcess1", "temperatureProcess2");
  static final List<String> COLUMNS_FORHBASE = Arrays.asList("productHumidity", "inputTemperatureProduct",
      "waterFlowProcess", "intensityFanProcess", "waterTemperatureProcess", "temperatureProcess1",
      "temperatureProcess2");

  public interface MyOptions extends StreamingOptions {

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);

    @Description("Over how long a time period should we average? (in minutes)")
    @Default.Double(60.0)
    Double getAveragingInterval();

    void setAveragingInterval(Double d);

    @Description("Simulation speedup factor. Use 1.0 if no speedup")
    @Default.Double(60.0)
    Double getSpeedupFactor();

    void setSpeedupFactor(Double d);

    @Description("HBase table name for running averages")
    @Default.String("runningAvgAnalysis")
    String getTableName();

    void setTableName(String output);

    @Description("HBase table name for current conditions")
    @Default.String("currentConditions")
    String getLoadTableName();

    void setLoadTableName(String output);

  }

  public static String getSchema(String schemaPath) throws IOException {
    ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(schemaPath, false));

    try (InputStream stream = Channels.newInputStream(chan)) {
      BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      StringBuilder dataBuilder = new StringBuilder();

      String line;
      while ((line = streamReader.readLine()) != null) {
        dataBuilder.append(line);
      }

      return dataBuilder.toString();
    }
  }

  public static class ConvertCsvToAvro extends DoFn<String, GenericRecord> {

    private String delimiter;
    private String schemaStr;

    public ConvertCsvToAvro(String schemaStr) {
      this.schemaStr = schemaStr;
      this.delimiter = ",";
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IllegalArgumentException {
      // Split CSV row into using delimiter
      String[] rowValues = c.element().split(delimiter);

      Schema schema = new Schema.Parser().parse(schemaStr);

      // Create Avro Generic Record
      GenericRecord genericRecord = new GenericData.Record(schema);
      List<Schema.Field> fields = schema.getFields();

      for (int index = 0; index < fields.size(); ++index) {
        String rowVal = rowValues[index];
        Schema.Field field = fields.get(index);
        Schema.Type fieldType = field.schema().getType();
        String fieldTypeName = "";
        if (Schema.Type.UNION.equals(fieldType)) {
          if (rowVal.isEmpty()) {
            fieldTypeName = "null";
          } else {
            // TODO: hardcoded for now. Need to accept whatever fieldtype it gives. not just
            // float
            // fieldTypeName = fieldType.values()[1] ...?
            fieldTypeName = "float";
          }
        } else {
          fieldTypeName = fieldType.getName().toLowerCase().toLowerCase();
        }

        switch (fieldTypeName) {
        case "string":
          genericRecord.put(field.name(), rowVal);
          break;
        case "boolean":
          genericRecord.put(field.name(), Boolean.valueOf(rowVal));
          break;
        case "int":
          genericRecord.put(field.name(), Integer.valueOf(rowVal));
          break;
        case "long":
          genericRecord.put(field.name(), Long.valueOf(rowVal));
          break;
        case "float":
          genericRecord.put(field.name(), Float.valueOf(rowVal));
          break;
        case "double":
          genericRecord.put(field.name(), Double.valueOf(rowVal));
          break;
        case "null":
          genericRecord.put(field.name(), null);
          break;
        default:
          throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
        }
      }
      c.output(genericRecord);
    }
  }

  public static class SetTimestampAsKey extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      List<String> info = new ArrayList<String>(Arrays.asList(c.element().split(",")));
      String timestamp = info.get(0);
      List<String> valueArray = info.subList(1, info.size());
      String valueStr = String.join(",", valueArray);
      c.output(KV.of(timestamp, valueStr));
    }
  }

  public static class ExtractValOfColumn extends DoFn<String, KV<String, Double>> {

    Integer index;

    public ExtractValOfColumn(Integer index) {
      this.index = index;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ArrayIndexOutOfBoundsException {
      List<String> info = new ArrayList<String>(Arrays.asList(c.element().split(",")));
      Double val = Double.valueOf(info.get(index));
      String key = String.valueOf(COLUMNS.get(index - 1));
      c.output(KV.of(key, val));
    }
  }

  public static class ConvertToStringKV extends DoFn<KV<String, Double>, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Double val = c.element().getValue();
      String strVal = String.valueOf(val);
      String key = c.element().getKey();
      String combinedVal = key + "," + strVal;
      c.output(KV.of(key, combinedVal));
    }
  }

  private static final DateTimeFormatter originalFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  public static class MakeMutation extends SimpleFunction<KV<String, String>, Mutation> {

    Integer index;
    Boolean isSensor;

    public MakeMutation(Boolean isSensor, Integer index) {
      this.index = index;
      this.isSensor = isSensor;
    }

    @Override
    public Mutation apply(KV<String, String> row) {
      String tstamp = row.getKey();
      String[] values = row.getValue().split(",");

      TemporalAccessor temporalAccessor = originalFormatter.parse(tstamp);
      String hbaseColumnName = minFormatter.format(temporalAccessor);
      String date_id = yyyMMddFormatter.format(temporalAccessor);
      String hour_id = hourFormatter.format(temporalAccessor);

      String measurementType;
      String valStr = values[index];
      // Check if inserting data to hbase for sensor or lab
      if (isSensor) {
        measurementType = COLUMNS.get(index);
      } else {
        measurementType = "productHumidity";
      }
      // rowkey design: [factory_id]#[oven_id]#[meter_id]#[yyyyMMdd]#[HH]
      // under the assumption that the common queries conducted by analysts are on
      // hourly basis
      // meter_id is same as the fieldName from original data (e.g.
      // temperatureProcess1)
      byte[] rowKey = ("001#001#" + measurementType + "#" + date_id + "#" + hour_id).getBytes(StandardCharsets.UTF_8);

      // Check if measurement type is "processIsOn"
      if (measurementType == "processIsOn") {
        // "1" = true, "0" = false
        Boolean value = "1".equals(valStr);
        return makeBatchMutationBools(rowKey, hbaseColumnName, value);
      } else {
        // all other values are doubles
        Double value = Double.parseDouble(valStr);
        return makeBatchMutationDoubs(rowKey, hbaseColumnName, value);
      }

    }
  }

  public static void main(String[] args) throws IOException, IllegalArgumentException {

    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    options.setStreaming(true);
    Pipeline p = Pipeline.create(options);
    final Configuration conf = HBaseConfiguration.create();

    // Build the Avro schema for the Avro output.
    File file = new File("avroSchema.avsc");
    String path = file.getAbsolutePath();
    String schemaJson = getSchema(path);
    Schema schema = new Schema.Parser().parse(schemaJson);

    /*
     * Batch Pipeline: send current conditions to both Avro and HBase
     */
    // Read Oven Sensor data
    PCollection<String> sensorDataFromKafka = p.apply("GetSensorDataMessages",
        KafkaIO.<Long, String>read().withBootstrapServers(KAFKA_SERVER).withTopic("sensor")
            .withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class)
            .withoutMetadata())
        .apply("ExtractValues", Values.<String>create());

    PCollection<KV<String, String>> sensorData = sensorDataFromKafka
        .apply("ByTimestamp", ParDo.of(new SetTimestampAsKey()))
        .apply("TimeWindow", Window.into(FixedWindows.of(Duration.standardSeconds(FIXED_WINDOW_SIZE))));

    // Read Lab Humidity data
    PCollection<KV<String, String>> labData = p
        .apply("GetLabDataMessages",
            KafkaIO.<Long, String>read().withBootstrapServers(KAFKA_SERVER).withTopic("lab")
                .withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class)
                .withoutMetadata())
        .apply("ExtractValues", Values.<String>create()).apply("ByTimestamp", ParDo.of(new SetTimestampAsKey()))
        .apply("TimeWindow", Window.into(FixedWindows.of(Duration.standardSeconds(FIXED_WINDOW_SIZE))));

    // Batch load to HBase
    for (int i = 0; i < COLUMNS.size(); i++) {
      sensorData.apply("ToHBaseMutation", MapElements.via(new MakeMutation(true, i)))//
          .apply("WriteCurrentConditionsToHBase", HBaseIO.write() //
              .withConfiguration(conf).withTableId(options.getLoadTableName()));
    }

    labData.apply("ToHBaseMutation", MapElements.via(new MakeMutation(false, 0)))//
        .apply("WriteCurrentConditionsToHBase", HBaseIO.write() //
            .withConfiguration(conf).withTableId(options.getLoadTableName()));

    // Merging two PCollections to one for writing to Avro
    final TupleTag<String> sensorTag = new TupleTag<String>();
    final TupleTag<String> labTag = new TupleTag<String>();

    PCollection<KV<String, CoGbkResult>> mergedData = KeyedPCollectionTuple.of(sensorTag, sensorData)
        .and(labTag, labData).apply("MergeLabAndSensor", CoGroupByKey.<String>create());

    PCollection<String> mergedTable = mergedData //
        .apply("ExtractValuesAndConvertToCSV", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kvRow = c.element();
            String timestamp = kvRow.getKey();
            List<String> timestampList = new ArrayList<String>();
            timestampList.add(timestamp);
            Iterable<String> sensorVals = kvRow.getValue().getAll(sensorTag);
            Iterable<String> labVals = kvRow.getValue().getAll(labTag);
            // TODO: This is a hacky way of doing it. Figure out if it's possible to convert
            // from Iterable to List more gracefully
            // Or if there is a way to combine the Iterables, instead of converting to Lists
            List<String> sensorList = new ArrayList<String>();
            List<String> labList = new ArrayList<String>();
            for (String sensorVal : sensorVals) {
              sensorList.add(sensorVal);
            }
            for (String labVal : labVals) {
              labList.add(labVal);
            }
            // if no lab result taken at this time, leave it as an empty string so schema
            // fits
            if (labList.isEmpty()) {
              labList.add("");
            }
            // List<String> sensorValsList = Lists.newArrayList(myIterator);
            List<String> combinedList = Stream.of(timestampList, labList, sensorList) //
                .flatMap(Collection::stream).collect(Collectors.toList());
            String combinedRow = String.join(",", combinedList);

            c.output(combinedRow);
          }
        }));

    // Write as Avro to local file.
    // TODO: send to hdfs directly; currently there's an error with AvroIO.to() not
    // recognizing "hdfs" file scheme
    mergedTable.apply("ConvertCSVtoAvro", ParDo.of(new ConvertCsvToAvro(schemaJson)))
        .setCoder(AvroCoder.of(GenericRecord.class, schema)).apply("WriteAsAvro", AvroIO.writeGenericRecords(schema)
            .to(options.getOutput()).withSuffix(".avro").withNumShards(1).withWindowedWrites());

    /*
     * Stream Pipeline: running average of all sensor values over 30 minute windows
     * (frequency of 2)
     */
    // Add sliding window to sensor data
    PCollection<String> currentSensorDataToSplit = sensorDataFromKafka //
        .apply("TimeWindow", Window.into(SlidingWindows //
            .of(Duration.standardSeconds(SLIDING_WINDOW_SIZE))
            .every(Duration.standardSeconds(SLIDING_WINDOW_SIZE / 2))));

    // Split each column to K,V for averaging, then find the average
    PCollection<KV<String, String>> inputTempProduct = currentSensorDataToSplit //
        .apply("ByTimestamp", ParDo.of(new ExtractValOfColumn(2))).apply("AvgByTimestamp", Mean.perKey())
        .apply("ConvertToString", ParDo.of(new ConvertToStringKV()));

    PCollection<KV<String, String>> waterFlowProcess = currentSensorDataToSplit //
        .apply("ByTimestamp", ParDo.of(new ExtractValOfColumn(3))).apply("AvgByTimestamp", Mean.perKey())
        .apply("ConvertToString", ParDo.of(new ConvertToStringKV()));

    PCollection<KV<String, String>> intensityFanProcess = currentSensorDataToSplit //
        .apply("ByTimestamp", ParDo.of(new ExtractValOfColumn(4))).apply("AvgByTimestamp", Mean.perKey())
        .apply("ConvertToString", ParDo.of(new ConvertToStringKV()));

    PCollection<KV<String, String>> waterTempProcess = currentSensorDataToSplit //
        .apply("ByTimestamp", ParDo.of(new ExtractValOfColumn(5))).apply("AvgByTimestamp", Mean.perKey())
        .apply("ConvertToString", ParDo.of(new ConvertToStringKV()));

    PCollection<KV<String, String>> tempProcess1 = currentSensorDataToSplit //
        .apply("ByTimestamp", ParDo.of(new ExtractValOfColumn(6))).apply("AvgByTimestamp", Mean.perKey())
        .apply("ConvertToString", ParDo.of(new ConvertToStringKV()));

    PCollection<KV<String, String>> tempProcess2 = currentSensorDataToSplit //
        .apply("ByTimestamp", ParDo.of(new ExtractValOfColumn(7))).apply("AvgByTimestamp", Mean.perKey())
        .apply("ConvertToString", ParDo.of(new ConvertToStringKV()));

    // Send out to kafka with topic=averages
    inputTempProduct.apply("PublishAvgsToKafka", KafkaIO.<String, String>write().withBootstrapServers(KAFKA_SERVER)
        .withTopic("averages").withKeySerializer(StringSerializer.class).withValueSerializer(StringSerializer.class));
    waterFlowProcess.apply("PublishAvgsToKafka", KafkaIO.<String, String>write().withBootstrapServers(KAFKA_SERVER)
        .withTopic("averages").withKeySerializer(StringSerializer.class).withValueSerializer(StringSerializer.class));
    intensityFanProcess.apply("PublishAvgsToKafka", KafkaIO.<String, String>write().withBootstrapServers(KAFKA_SERVER)
        .withTopic("averages").withKeySerializer(StringSerializer.class).withValueSerializer(StringSerializer.class));
    waterTempProcess.apply("PublishAvgsToKafka", KafkaIO.<String, String>write().withBootstrapServers(KAFKA_SERVER)
        .withTopic("averages").withKeySerializer(StringSerializer.class).withValueSerializer(StringSerializer.class));
    tempProcess1.apply("PublishAvgsToKafka", KafkaIO.<String, String>write().withBootstrapServers(KAFKA_SERVER)
        .withTopic("averages").withKeySerializer(StringSerializer.class).withValueSerializer(StringSerializer.class));
    tempProcess2.apply("PublishAvgsToKafka", KafkaIO.<String, String>write().withBootstrapServers(KAFKA_SERVER)
        .withTopic("averages").withKeySerializer(StringSerializer.class).withValueSerializer(StringSerializer.class));

    // Write to HBase
    inputTempProduct.apply("ToHBaseMutation", MapElements.via(new SimpleFunction<KV<String, String>, Mutation>() {
      @Override
      public Mutation apply(KV<String, String> input) {
        return makeMutation(getThisInstantFormatted(streamFormatter), input.getValue());
      }
    })).apply("WriteAvgsToHBase", HBaseIO.write() //
        .withConfiguration(conf).withTableId(options.getTableName()));

    waterFlowProcess.apply("ToHBaseMutation", MapElements.via(new SimpleFunction<KV<String, String>, Mutation>() {
      @Override
      public Mutation apply(KV<String, String> input) {
        return makeMutation(getThisInstantFormatted(streamFormatter), input.getValue());
      }
    })).apply("WriteAvgsToHBase", HBaseIO.write() //
        .withConfiguration(conf).withTableId(options.getTableName()));

    intensityFanProcess.apply("ToHBaseMutation", MapElements.via(new SimpleFunction<KV<String, String>, Mutation>() {
      @Override
      public Mutation apply(KV<String, String> input) {
        return makeMutation(getThisInstantFormatted(streamFormatter), input.getValue());
      }
    })).apply("WriteAvgsToHBase", HBaseIO.write() //
        .withConfiguration(conf).withTableId(options.getTableName()));

    waterTempProcess.apply("ToHBaseMutation", MapElements.via(new SimpleFunction<KV<String, String>, Mutation>() {
      @Override
      public Mutation apply(KV<String, String> input) {
        return makeMutation(getThisInstantFormatted(streamFormatter), input.getValue());
      }
    })).apply("WriteAvgsToHBase", HBaseIO.write() //
        .withConfiguration(conf).withTableId(options.getTableName()));

    tempProcess1.apply("ToHBaseMutation", MapElements.via(new SimpleFunction<KV<String, String>, Mutation>() {
      @Override
      public Mutation apply(KV<String, String> input) {
        return makeMutation(getThisInstantFormatted(streamFormatter), input.getValue());
      }
    })).apply("WriteAvgsToHBase", HBaseIO.write() //
        .withConfiguration(conf).withTableId(options.getTableName()));

    tempProcess2.apply("ToHBaseMutation", MapElements.via(new SimpleFunction<KV<String, String>, Mutation>() {
      @Override
      public Mutation apply(KV<String, String> input) {
        return makeMutation(getThisInstantFormatted(streamFormatter), input.getValue());
      }
    })).apply("WriteAvgsToHBase", HBaseIO.write() //
        .withConfiguration(conf).withTableId(options.getTableName()));

    p.run();
  }

  private static Mutation makeBatchMutationDoubs(byte[] rowkey, String columnName, Double value) {
    return new Put(rowkey).addColumn(COLUMN_FAMILY, Bytes.toBytes(columnName), Bytes.toBytes(value));
  }

  private static Mutation makeBatchMutationBools(byte[] rowkey, String columnName, Boolean value) {
    return new Put(rowkey).addColumn(COLUMN_FAMILY, Bytes.toBytes(columnName), Bytes.toBytes(value));
  }

  private static Mutation makeMutation(String key, String value) {
    // rowkey design: [factory_id]#[oven_id]#[timestamp of running avg]
    byte[] rowKey = ("001#001#" + key).getBytes(StandardCharsets.UTF_8);
    String[] keyval = value.split(",");
    String columnName = keyval[0];
    Double dValue = Double.parseDouble(keyval[1]);
    return new Put(rowKey).addColumn(COLUMN_FAMILY, Bytes.toBytes(columnName), Bytes.toBytes(dValue));
  }

  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("METER");

  // [yyyyMMdd]#[HH]
  private static final DateTimeFormatter yyyMMddFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      .withZone(ZoneId.systemDefault());

  private static final DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("HH")
      .withZone(ZoneId.systemDefault());

  private static final DateTimeFormatter minFormatter = DateTimeFormatter.ofPattern("mm")
      .withZone(ZoneId.systemDefault());

  private static final DateTimeFormatter streamFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
      .withZone(ZoneId.systemDefault());

  private static String getThisInstantFormatted(DateTimeFormatter formatter) {
    Instant instant = Instant.now();
    return formatter.format(instant);
  }

}
