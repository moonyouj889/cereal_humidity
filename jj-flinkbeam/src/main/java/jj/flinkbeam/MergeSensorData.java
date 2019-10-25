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

import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
// import org.apache.beam.runners.flink.FlinkPipelineOptions;

import java.io.File;
import org.joda.time.Duration;
import org.apache.avro.Schema;
// import org.apache.avro.Schema.Type;
// import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
// import java.util.Arrays;
// import java.util.List;
import java.io.BufferedReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

/*
mvn compile exec:java -Dexec.mainClass=jj.flinkbeam.MergeSensorData \
    -Pflink-runner \
    -Djava.util.logging.config.file=src/main/resources/logging.properties \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/kafkamsgs \
      --flinkMaster=localhost" 

mvn -e -X clean package exec:java -Dexec.mainClass=jj.flinkbeam.MergeSensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/kafkamsgs \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar" \
    -Djava.util.logging.config.file=src/main/resources/logging.properties \

mvn clean package exec:java -Dexec.mainClass=jj.flinkbeam.MergeSensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/kafkamsgs \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar" \
    -Djava.util.logging.config.file=src/main/resources/logging.properties

You can monitor the running job by visiting the Flink dashboard at http://localhost:8081
*/
@SuppressWarnings("serial")
public class MergeSensorData {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // window size in seconds (in reality, would use it for daily batch load)
  static final int WINDOW_SIZE = 60; 

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
  }

  public static String getSchema(String schemaPath) throws IOException {
    ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
        schemaPath, false));

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
    public void processElement(ProcessContext ctx) throws IllegalArgumentException {
      // Split CSV row into using delimiter
      String[] rowValues = ctx.element().split(delimiter);

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
            // TODO: hardcoded for now. Need to accept whatever fieldtype it gives. not just float
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
          LOG.error("Data transformation doesn't support: " + fieldType);
          throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
        }
      }
      ctx.output(genericRecord);
    }
  }

  public static class SplitToKV extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      List<String> info = new ArrayList<String>(Arrays.asList(c.element().split(",")));
      String timestamp = info.get(0);
      List<String> valueArray = info.subList(1, info.size());
      String valueStr = String.join(",", valueArray);
      c.output(KV.of(timestamp, valueStr));
    }
  }


  public static void main(String[] args) throws IOException, IllegalArgumentException {

    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    options.setStreaming(true);
    Pipeline p = Pipeline.create(options);

    // Build the Avro schema for the Avro output.
    // Schema schema = null;
    File file = new File("avroSchema.avsc");
    String path = file.getAbsolutePath();
    String schemaJson = getSchema(path);
    Schema schema = new Schema.Parser().parse(schemaJson);

    // Duration averagingInterval = Duration.millis(Math.round(1000 * 60 * (options.getAveragingInterval())));

    // Read Oven Sensor data
    PCollection<KV<String,String>> sensorData = p
      .apply("GetMessages", KafkaIO.<Long, String>read()
          .withBootstrapServers("localhost:9092")
          .withTopic("sensor")
          .withKeyDeserializer(LongDeserializer.class)
          .withValueDeserializer(StringDeserializer.class).withoutMetadata())
      .apply("ExtractValues", Values.<String>create())
      .apply("ByTimestamp", ParDo.of(new SplitToKV()))
      .apply("TimeWindow", Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE))));

    // Read Lab Humidity data
    PCollection<KV<String,String>> labData = p
      .apply("GetMessages", KafkaIO.<Long, String>read()
          .withBootstrapServers("localhost:9092") 
          .withTopic("lab")
          .withKeyDeserializer(LongDeserializer.class)
          .withValueDeserializer(StringDeserializer.class).withoutMetadata())
      .apply("ExtractValues", Values.<String>create())
      .apply("ByTimestamp", ParDo.of(new SplitToKV()))
      .apply("TimeWindow", Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE))));


    final TupleTag<String> sensorTag = new TupleTag<String>();
    final TupleTag<String> labTag = new TupleTag<String>();

    // Merge collection values into a CoGbkResult collection.
    PCollection<KV<String, CoGbkResult>> mergedData =
      KeyedPCollectionTuple.of(sensorTag, sensorData)
                          .and(labTag, labData)
                          .apply(CoGroupByKey.<String>create());


    PCollection<String> mergedTable = mergedData //
      .apply("ExtractValues", ParDo.of(
        new DoFn<KV<String, CoGbkResult>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            KV<String, CoGbkResult> kvRow = c.element();
            String timestamp = kvRow.getKey();
            List<String> timestampList = new ArrayList<String>();
            timestampList.add(timestamp);
            Iterable<String> sensorVals = kvRow.getValue().getAll(sensorTag);
            Iterable<String> labVals = kvRow.getValue().getAll(labTag);
            // TODO: This is a hacky way of doing it. Figure out if it's possible to convert from Iterable to List more gracefully
            // Or if there is a way to combine the Iterables, instead of converting to Lists
            List<String> sensorList = new ArrayList<String>();
            List<String> labList = new ArrayList<String>();
            for (String sensorVal : sensorVals) {sensorList.add(sensorVal);}
            for (String labVal : labVals) {labList.add(labVal);}
            // if no lab result taken at this time, leave it as an empty string so schema fits
            if (labList.isEmpty()) {labList.add("");}
            // List<String> sensorValsList = Lists.newArrayList(myIterator);
            List<String> combinedList = Stream.of(timestampList, labList, sensorList) //
                                               .flatMap(Collection::stream)
                                               .collect(Collectors.toList());
            String combinedRow = String.join(",", combinedList);

            c.output(combinedRow);
        }}));

    mergedTable.apply("ConvertCSVtoAvro", ParDo.of(new ConvertCsvToAvro(schemaJson)))
               .setCoder(AvroCoder.of(GenericRecord.class, schema))
               .apply("WriteAsAvro", AvroIO.writeGenericRecords(schema)
                        .to(options.getOutput()).withSuffix(".avro")
                        .withNumShards(1).withWindowedWrites());

    p.run();
  }
}
