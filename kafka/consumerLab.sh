# make sure to fix the .properties file so the server isn't localhost:9092 for production
kafka-console-consumer --bootstrap-server localhost:9092 --topic lab --from-beginning