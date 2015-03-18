DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOT_DIR=${DIR}/..

${ROOT_DIR}/deploy/confluent/bin/kafka-avro-console-producer \
             --broker-list localhost:9092 --topic questions \
             --compression-codec snappy \
             --property schema.registry.url=http://localhost:9081 \
             --property value.schema="`cat target/generated-schema/avro/FortuneRequest.avsc`" < test-data/requests.json
