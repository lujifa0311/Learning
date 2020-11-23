bin/kafka-topics.sh --zookeeper  localhost:2181 --describe --topic customer-stat-update

watch -n 5 -d './kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group customer-stat-update-consumer-group_su1 | grep customer-stat-update'

kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group  customer-stat-update-consumer-group_su1 --topic customer-stat-update
