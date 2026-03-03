# Kafka Demo
# start the zookeeper service(after downloading and extracting kafka folder)
bin/zookeeper-server-start.sh config/zookeeper.properties
# start the kafka broker service 
bin/kafka-server-start.sh config/server.properties

# Create a topic using the cli kafka-topics.sh 
kafka-topics.sh --create --topic DemoTopic --partitions 1 --replication-factor 1 --zookeeper localhost:2181

# with partitions 2 and replication factor 2
kafka-topics.sh --create --topic DemoTopic1 --partitions 2 --replication-factor 2 --zookeeper localhost:2181

# to display all topics 
kafka-topics.sh --list --zookeeper localhost:2181

# to display the details of all topics using the command line tool
kafka-topics.sh --describe --zookeeper localhost:2181

# display details of a particular topic 
kafka-topics.sh --describe --topic DemoTopic1 --zookeeper localhost:9092

# delete a topic from kafka cluster
kafka-topics.sh --zookeeper localhost:2181 --delete  --topic DemoTopic1

# to write messages into the kafka console producer.sh
kafka-console-producer.sh --broker-list localhost:9092 --topic DemoTopic
(to stop the console producer use ctrl+c)

# to consume the messages use kafka-console-consumer.sh 
kafka-console-consumer.sh --zookeeper localhost:2181 --topic DemoTopic --from-beginning

# to consume the latest messages from the topic,open a new terminal and execute
kafka-console-consumer.sh --zookeeper localhost:2181 --topic DemoTopic 

# boostrap_servers_confg --> provides list of kafka brokers available in kafka cluster

In latest version zookeeper is removed, so instead of zookeeper in all commands
we use --bootstrap-server 