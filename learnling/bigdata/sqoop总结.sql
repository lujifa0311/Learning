--测试Sqoop是否能成功连接数据库
bin/sqoop list-databases --connect jdbc:mysql://hadoop102:3306 --username root --password 000000

--导入
--非大数据集群（RDBMS）向大数据集群（HDFS，HIVE，HBASE）中传输数据，叫做：导入，即使用import关键字

--RDBMS到HDFS  全部
bin/sqoop import \
--connect jdbc:mysql://hadoop102:3306/company \
--username root \
--password 000000 \
--table staff \
--target-dir /user/company \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by "\t"

--查询导入
bin/sqoop import \
--connect jdbc:mysql://hadoop102:3306/company \
--username root \
--password 000000 \
--target-dir /user/company \  导入文件夹
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by "\t" \  分隔符
--query 'select name,sex from staff where id <=1 and $CONDITIONS;'   条件查询

--导入个别字段
$ bin/sqoop import \
--connect jdbc:mysql://hadoop102:3306/company \
--username root \
--password 000000 \
--target-dir /user/company \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by "\t" \
--columns id,sex \
--table staff


--RDBMS到Hive
$ bin/sqoop import \
--connect jdbc:mysql://hadoop102:3306/company \
--username root \
--password 000000 \
--table staff \
--num-mappers 1 \
--hive-import \
--fields-terminated-by "\t" \
--hive-overwrite \
--hive-table staff_hive




