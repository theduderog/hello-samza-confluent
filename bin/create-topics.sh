DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#Create metrics topic
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic samza_metrics --create --partitions 1 --replication-factor 1

#Create topic for fortune requests
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic questions --create --partitions 1 --replication-factor 1
#Create topic for fortunes
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic fortunes --create --partitions 1 --replication-factor 1

