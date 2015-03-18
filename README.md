Testing integration of Samza with Avro + Confluent Platform

# Notes
* Confluent Platform and Camus require that all application schemas include a timestamp field (header.timestamp) that's used for partitioning messages in HDFS.  To avoid repeating the same header type definition, I'm using Apache Velocity to include a common definition.  We could use the import option of the avro-maven-plugin to make the build work with a common header schema but then we won't have self-contained schema files that we can upload to the registry.
* This project includes a patched version of io.confluent:kafka-avro-serializer to support decoding to SpecificRecords.  The work comes from [@danharvey](https://github.com/danharvey/schema-registry/tree/specific-avro-decoder) and is merged with current master on [this branch](https://github.com/theduderog/schema-registry/tree/specific-avro-decoder).
* I've heavily modified the [grid](https://github.com/apache/samza-hello-samza/blob/master/bin/grid) script from hello-samza to work with Confluent Platform.

# Dev Setup

## Build

	#Do this one time only
	./bin/grid install all
	
	mvn package && rm -rf deploy/samza && mkdir deploy/samza && tar -xvf ./target/hello-samza-confluent-0.0.1-dist.tar.gz -C deploy/samza
	
## Deployment

	#Start grid
	rm -rf /tmp/kafka-logs/ && rm -rf /tmp/zookeeper/
	./bin/grid start all
	./bin/create-topics.sh
	./bin/load-topics.sh
   
	#View questions
	./deploy/confluent/bin/kafka-avro-console-consumer --topic questions \
             --zookeeper localhost:2181 \
             --property schema.registry.url=http://localhost:9081 \
             --from-beginning
             
	#Run the main job
	./bin/start-job.sh seer 

    #View answers
    ./deploy/confluent/bin/kafka-avro-console-consumer --topic fortunes \
             --zookeeper localhost:2181 \
             --property schema.registry.url=http://localhost:9081 \
             --from-beginning
