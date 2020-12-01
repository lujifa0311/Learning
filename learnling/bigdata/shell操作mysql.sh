#!/bin/bash
HOSTNAME="127.0.0.1"  #数据库信息
PORT="3306"
USERNAME="root"
PASSWORD="123456789"

DBNAME="test_db"  #数据库名称
TABLENAME="test_table" #数据库中表的名称

show_database="show databases" #显示所有数据库
show_table="show tables" #显示当前数据库中所有表

#创建数据库
create_db_sql="create database IF NOT EXISTS ${DBNAME}"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} -e "${create_db_sql}"

#查询所有数据库
echo -e "显示所有数据库\n"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} -e "${show_database}"

#创建表
create_table_sql="create table IF NOT EXISTS ${TABLENAME} ( name varchar(20), id int(11) default 0 )"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${create_table_sql}"

#查询所有表
echo -e "显示当前数据库下所有数据表\n"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${show_table}"

#插入数据
insert_sql="insert into ${TABLENAME} values('wangpandong',1001)"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"

insert_sql="insert into ${TABLENAME} values('zhangsan',1002)"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"

insert_sql="insert into ${TABLENAME} values('lisi',1003)"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"

insert_sql="insert into ${TABLENAME} values('wangwu',1004)"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"

#查询
echo -e "插入之后查询数据.\n" #不加-e则\n认为是非转义字符输出
select_sql="select * from ${TABLENAME}"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${select_sql}"

#更新数据
update_sql="update ${TABLENAME} set name='aobama' where id = 1002 "
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${update_sql}"
echo -e "更新之后查询数据\n"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${select_sql}"

#删除表中一个内容
delete_sql="delete from ${TABLENAME} where name = 'wangpandong'"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${delete_sql}"
echo -e "删除wangpandong之后查询数据\n"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${select_sql}"

#删除表中所有内容
delete_sql="delete from ${TABLENAME}"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${delete_sql}"
echo -e "删除表中所有内容之后查询数据\n"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${select_sql}"

#删除整个表
delete_table="drop table ${TABLENAME}"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${delete_table}"
echo -e "删除表之后查询表\n"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${show_table}"