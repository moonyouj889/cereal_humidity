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

package jj.flinkbeam;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.file.DataFileReader;
import java.io.File;
import org.apache.avro.Schema;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

/*
mvn compile exec:java -Dexec.mainClass=jj.flinkbeam.AvroToLog \
    -Djava.util.logging.config.file=src/main/resources/logging.properties
*/

public class AvroToLog {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String FILEPATH = "/Users/juliejung/Documents/deProjects/cereal_humidity/py-spark/avroFiles/kafkamsgs2019-10-26T13:06:00.000Z-2019-10-26T13:12:00.000Z-pane-0-last-00000-of-00001.avro";

  public static void main(String[] args) throws IOException {

    Schema schema = new Schema.Parser().parse(
        "{\"amespace\": \"foodDrying.avro\",\"type\": \"record\",\"name\": \"sensorAndLab\",\"fields\": [{\"name\": \"timestamp\", \"type\": \"string\"},{\"name\": \"productHumidity\",  \"type\": [\"null\", \"float\"]},{\"name\": \"processOn\",  \"type\": \"boolean\"},{\"name\": \"inputTemperatureProduct\", \"type\": \"float\"},{\"name\": \"waterFlowProcess\", \"type\": \"float\"},{\"name\": \"intensityFanProcess\", \"type\": \"float\"},{\"name\": \"waterTemperatureProcess\", \"type\": \"float\"},{\"name\": \"temperatureProcess1\", \"type\": \"float\"},{\"name\": \"temperatureProcess2\", \"type\": \"float\"}]}");
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    File datafile = new File(FILEPATH);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(datafile, datumReader);
    GenericRecord row = null;
    while (dataFileReader.hasNext()) {
      // Reuse user object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with
      // many items.
      row = dataFileReader.next(row);
      System.out.println(row.toString());
      // LOG.info(row.toString());
    }
  }
}