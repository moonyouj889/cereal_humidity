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
import java.util.ImmutableList;

import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
// import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;

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
import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

/*
mvn compile exec:java -Dexec.mainClass=jj.flinkbeam.SensorData \
    -Pflink-runner \
    -Djava.util.logging.config.file=src/main/resources/logging.properties \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/kafkamsgs \
      --flinkMaster=localhost" 

mvn -e -X clean package exec:java -Dexec.mainClass=jj.flinkbeam.SensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/kafkamsgs \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar" \
    -Djava.util.logging.config.file=src/main/resources/logging.properties \

mvn clean package exec:java -Dexec.mainClass=jj.flinkbeam.SensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=/tmp/kafkamsgs \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar" \
    -Djava.util.logging.config.file=src/main/resources/logging.properties

mvn clean package exec:java -Dexec.mainClass=jj.flinkbeam.SensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --output=hdfs://localhost:9000/cereal/data \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar \
      --hdfsConfiguration=fs.defaultFS:hdfs://localhost:9000 "
    -Djava.util.logging.config.file=src/main/resources/logging.properties

mvn clean package exec:java -Dexec.mainClass=jj.flinkbeam.SensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --flinkMaster=localhost \
      --hdfsConfiguration=[{\"fs.defaultFS\":\"hdfs://localhost:9000\"}]  \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar " \
    -Djava.util.logging.config.file=src/main/resources/logging.properties

mvn clean package exec:java -Dexec.mainClass=jj.flinkbeam.SensorData \
    -Pflink-runner \
    -Dexec.args="--runner=FlinkRunner \
      --flinkMaster=localhost \
      --filesToStage=target/jj-flinkbeam-bundled-1.0-SNAPSHOT.jar " \
    -Djava.util.logging.config.file=src/main/resources/logging.properties

You can monitor the running job by visiting the Flink dashboard at http://localhost:8081
*/
@SuppressWarnings("serial")
public class SensorData {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  // public void setUp() throws Exception {
  //   Configuration configuration = new Configuration();
  //   configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.getRoot().getAbsolutePath());
  //   MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
  //   hdfsCluster = builder.build();
  //   URI hdfsClusterBaseUri = new URI(configuration.get("fs.defaultFS") + "/");
  //   fileSystem = new HadoopFileSystem(configuration);
  // }

  public interface MyOptions extends StreamingOptions, HadoopFileSystemOptions {

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Default.String("hdfs://localhost:9000/cereal/data")
    String getOutput();

    void setOutput(String value);

    // @Description("Over how long a time period should we average? (in minutes)")
    // @Default.Double(60.0)
    // Double getAveragingInterval();

    // void setAveragingInterval(Double d);

    // @Description("Simulation speedup factor. Use 1.0 if no speedup")
    // @Default.Double(60.0)
    // Double getSpeedupFactor();

    // void setSpeedupFactor(Double d);
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
        Schema.Field field = fields.get(index);
        String fieldType = field.schema().getType().getName().toLowerCase();

        switch (fieldType) {
        case "string":
          genericRecord.put(field.name(), rowValues[index]);
          break;
        case "boolean":
          genericRecord.put(field.name(), Boolean.valueOf(rowValues[index]));
          break;
        case "int":
          genericRecord.put(field.name(), Integer.valueOf(rowValues[index]));
          break;
        case "long":
          genericRecord.put(field.name(), Long.valueOf(rowValues[index]));
          break;
        case "float":
          genericRecord.put(field.name(), Float.valueOf(rowValues[index]));
          break;
        case "double":
          genericRecord.put(field.name(), Double.valueOf(rowValues[index]));
          break;
        default:
          throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
        }
      }
      ctx.output(genericRecord);
    }
  }


  public static void main(String[] args) throws IOException, IllegalArgumentException {
    // String[] hdfsArgs = new String[]{""}; 
    
    // String[] hdfsArgs = new String[]{ "--hdfsConfiguration=[{\"fs.defaultFS\" : \"hdfs://host:port\"}]"}; 
    // String[] hdfsArgs = new String[]{ "--hdfsConfiguration=[{\"fs.hdfs.impl\": org.apache.hadoop.hdfs.DistributedFileSystem.class.getName(), \"fs.file.impl\": org.apache.hadoop.fs.LocalFileSystem.class.getName()}]"}; 


    // int length = hdfsArgs.length + args.length;
    // String[] combinedArgs = new String[length];
    // int pos = 0;
    // for (String element : hdfsArgs) {
    //     combinedArgs[pos] = element;
    //     pos++;
    // }
    // for (String element : args) {
    //     combinedArgs[pos] = element;
    //     pos++;
    // }

    // hadoopConfig.set("fs.hdfs.impl", 
    // org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    // );
    // hadoopConfig.set("fs.file.impl",
    //     org.apache.hadoop.fs.LocalFileSystem.class.getName()
    // );

    // LOG.error("COMBINED ARGS" + Arrays.toString(combinedArgs));

    // options = PipelineOptionsFactory .fromArgs(combinedArgs) .withValidation() .as(HadoopFileSystemOptions.class); 
    // Pipeline pipeline = Pipeline.create(options);
    MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    options.setStreaming(true);
    options.setHdfsConfiguration(ArrayList([{"fs.defaultFS" : "hdfs://host:port"}]));
    // options.setHdfsConfiguration(ImmutableList.of(fileSystem.fileSystem.getConf()));
    Pipeline p = Pipeline.create(options);

    // Build the Avro schema for the Avro output.
    File file = new File("woHumid.avsc");
    String path = file.getAbsolutePath();
    String schemaJson1 = getSchema(path);
    Schema schema = new Schema.Parser().parse(schemaJson1);

    LOG.error("HERE IS THE OUTPUTDIR! " + options.getOutput());
    // ResourceId outdir = FileSystems.matchNewResource(options.getOutput(), true);

    // Duration averagingInterval = Duration.millis(Math.round(1000 * 60 * (options.getAveragingInterval())));

    PCollection<String> currentConditions = p //
        .apply("GetMessages", KafkaIO.<Long, String>read(). //
            withBootstrapServers("localhost:9092"). // s
            withTopic("sensor"). //
            withKeyDeserializer(LongDeserializer.class). //
            withValueDeserializer(StringDeserializer.class).withoutMetadata())
        .apply(Values.<String>create());


    // currentConditions.apply("Convert CSV to Avro formatted data", ParDo.of(new ConvertCsvToAvro(schemaJson1))) /
    //     .setCoder(AvroCoder.of(GenericRecord.class, schema));

    currentConditions.apply("TimeWindow", Window.into(FixedWindows//
        .of(Duration.standardSeconds(60)))) //
        // .apply("Convert CSV to Avro formatted data", ParDo.of(new ConvertCsvToAvro(schemaJson1)))
        // .setCoder(AvroCoder.of(GenericRecord.class, schema)) //
        // .apply("WriteAsAvro", AvroIO.writeGenericRecords(schema) //
        // .to(options.getOutput()).withSuffix(".avro")//
        // .withNumShards(1).withWindowedWrites());
        .apply("WriteMessage", TextIO.write() //
            .withWindowedWrites() //
            .to("hdfs://127.0.0.1:9000/cereal/data") //
            .withNumShards(1));
    // .withCodec(CodecFactory.snappyCodec()) //
    p.run();
  }
}