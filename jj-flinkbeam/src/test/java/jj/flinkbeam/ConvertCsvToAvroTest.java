package jj.flinkbeam;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
// import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.testing.TestPipeline;

import jj.flinkbeam.SensorData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.file.DataFileReader;
import java.io.File;
import org.apache.avro.Schema;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.io.BufferedReader;
import org.apache.beam.sdk.io.FileSystems;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertCsvToAvroTest {
  private static final Logger LOG = LoggerFactory.getLogger(ConvertCsvToAvroTest.class);

  // public static String getSchema(String schemaPath) throws IOException {
  //   ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
  //       schemaPath, false));

  //   try (InputStream stream = Channels.newInputStream(chan)) {
  //     BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
  //     StringBuilder dataBuilder = new StringBuilder();

  //     String line;
  //     while ((line = streamReader.readLine()) != null) {
  //       dataBuilder.append(line);
  //     }

  //     return dataBuilder.toString();
  //   }
  // }

  @Test
  public void convertCsvToAvroTester() throws IOException{
    // SensorData testCase = new SensorData();
    // ConvertCsvToAvro myDoFn = new testCase.ConvertCsvToAvro();

    // DoFnTester<String, GenericRecord> fnTester = DoFnTester.of(testCase.ConvertCsvToAvro());
    // String testInput = "2014-06-11T13:53:00-04:00,0,45.15,11.333333333333334,0.0,34.85,22.85,23.75";
    // List<GenericRecord> testOutputs = fnTester.processBundle(testInput);
    // GenericRecord record1 = new GenericRecord();
    // record1.put(String "timestamp", String "2014-06-11T13:53:00-04:00");
    // record1.put(String "timestamp", String "2014-06-11T13:53:00-04:00")
    // Assert.assertThat(testOutputs, Matchers.hasItems());


    // File schemafile = new File("woHumid2.avsc");
    // File datafile = new File("kafkamsgs2019-10-21T15:03:00.000Z-2019-10-21T15:04:00.000Z-pane-0-last-00000-of-00001.avro");
    // String path = schemafile.getAbsolutePath();
    // String schemaJson1 = getSchema(path);
    // Schema schema = new Schema.Parser().parse(schemaJson1);
    // Deserialize users from disk
    // Schema schema = new Schema.Parser().parse("{\"amespace\": \"foodDrying.avro\",\"type\": \"record\",\"name\": \"sensorAndLab\",\"fields\": [{\"name\": \"timestamp\", \"type\": \"string\"},{\"name\": \"processOn\",  \"type\": \"boolean\"},{\"name\": \"inputTemperatureProduct\", \"type\": \"float\"},{\"name\": \"waterFlowProcess\", \"type\": \"float\"},{\"name\": \"intensityFanProcess\", \"type\": \"float\"},{\"name\": \"waterTemperatureProcess\", \"type\": \"float\"},{\"name\": \"temperatureProcess1\", \"type\": \"float\"},{\"name\": \"temperatureProcess2\", \"type\": \"float\"}]}");
    // DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    // File datafile = new File("/home/julie/Documents/deProjects/foodDrying/kafkamsgs2019-10-21T15:03:00.000Z-2019-10-21T15:04:00.000Z-pane-0-last-00000-of-00001.txt");
    // DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(datafile, datumReader);
    // GenericRecord row = null;
    // while (dataFileReader.hasNext()) {
    //   // Reuse user object by passing it to next(). This saves us from
    //   // allocating and garbage collecting many objects for files with
    //   // many items.
    //   row = dataFileReader.next(row);
    //   LOG.info(row.toString());
    // }

    assertTrue(true);

    // Pipeline p = TestPipeline.create();
  }
}
