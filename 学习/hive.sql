----建表

--PARQUET类型
CREATE TABLE parquet_test (
 id int,
 str string,
 mp MAP<STRING,STRING>,
 lst ARRAY<STRING>,
 strct STRUCT<A:STRING,B:STRING>) 
PARTITIONED BY (part string)
STORED AS PARQUET

--AVRO类型
CREATE TABLE parquet_test (
 id int,
 str string,
 mp MAP<STRING,STRING>,
 lst ARRAY<STRING>,
 strct STRUCT<A:STRING,B:STRING>) 
PARTITIONED BY (part string)
STORED AS AVRO

--TEXTFILE类型
CREATE TABLE test_serializer(string1 STRING,
                             list1 ARRAY<STRING>,
                             map1 MAP<STRING,INT>,
                             struct1 STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>,
                             union1 uniontype<FLOAT, BOOLEAN, STRING>,
                             enum1 STRING)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY ':' MAP KEYS TERMINATED BY '#' LINES TERMINATED BY '\n'
 STORED AS TEXTFILE;
 
--修改分区
alter table etl_update_customer_value_tag partition(create_day=20200608) rename to partition(create_day=2020060801);

--增加表的字段
alter table test add columns(字段名称 数据类型);

--修改表的名字
ALTER TABLE old_name RENAME TO new_name;


