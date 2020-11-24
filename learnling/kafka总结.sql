--创建一个topic 20个分区  副本数为2
bin/kafka-topics.sh --zookeeper node01:2181 --create --topic t_cdr --partitions 30  --replication-factor 2

--查找topic/查看topic列表
/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep unbound

--查看消费的数据/控制台消费topic的数据
/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dml_etl_unbound_customer --from-beginning

--控制台向topic生产数据
bin/kafka-console-producer.sh --broker-list node86:9092 --topic t_cdr

--查看topic结构(分区数 副本数)
bin/kafka-topics.sh --zookeeper  localhost:2181 --describe --topic customer-stat-update

--实时监控消费情况()
watch -n 5 -d 'binbin//kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group customer-stat-update-consumer-group_su1 | grep customer-stat-update'

--监控消费情况
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group  customer-stat-update-consumer-group_su1 --topic customer-stat-update
--topic：创建时topic名称
--pid：分区编号
--offset：表示该parition已经消费了多少条message
--logSize：表示该partition已经写了多少条message
--Lag：表示有多少条message没有被消费。
--Owner：表示消费者

--查看topic某分区偏移量最大（小）值
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --topic hive-mdatabase-hostsltable  --time -1 --broker-list node86:9092 --partitions 0
注： time为-1时表示最大值，time为-2时表示最小值

--增加topic分区数 为topic t_cdr 增加10个分区
bin/kafka-topics.sh --zookeeper node01:2181  --alter --topic t_cdr --partitions 10

