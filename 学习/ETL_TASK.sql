spark-shell --master yarn --deploy-mode client --num-executors 6 --executor-memory 2G --driver-memory 1G --executor-cores 1 \
--jars ./libs/kms-1.0-SNAPSHOT.jar,./libs/kudu-spark2_2.11-1.10.0-cdh6.3.0.jar,./libs/mysql-connector-java-8.0.16.jar \
--customer_identity
load_f309_dwm_wechat_bind
load_f502_mnp_memberinfo
load_f313_dwm_tmall_bind
load_d301_dwm_customer
|
etl_pull_customer_identity


etl_pull_customer
etl_pull_customer_identity
|
etl_update_customer_identity


etl_pull_customer
etl_pull_customer_identity
customer_attr
|
customer_identity


--customer
load_d203_dwm_store
load_d301_dwm_customer
etl_pull_customer_identity
|
etl_pull_customer

|
customer

--customer_attribute

etl_pull_customer
|
customer_attr
|
customer_attr_update

--customer_tag
ods_label_definition
|
etl_pull_tags
|
tags


load_customer_tags
load_hyper_id
etl_pull_customer
customer_attr_update
customer_identity
|
etl_pull_customer_tag


etl_pull_customer_tag
etl_pull_tags
tags
|
etl_update_customer_tag
|
insert_customer_tag


--customer_value_tag

load_value_tag_definition
|
etl_pull_value_tag
|
external_attr_config
|
etl_pull_external_attr_config
|
customer_extended_attr_definition


load_customer_value_tag
load_hyper_id
etl_pull_customer
insert_customer_tag
customer_attr_update
customer_identity
|
etl_pull_customer_value_tag


etl_pull_customer_value_tag
etl_pull_external_attr_config
etl_pull_full_dim
load_f1104_pre_hot_click
|
etl_update_customer_value_tag


update_customer_value_tag_attribute
|
customer_extended_attr_all
|
customer_extended_attr_hbase



etl_update_customer_value_tag
load_f1104_pre_hot_click
|
etl_update_customer_value_tag_attribute
|
update_customer_value_tag_attribute








--群组
customer
customer_attr
customer_attr_update
customer_event_bindwechat
customer_event_mini_program_open
customer_event_coupon_redeemed
customer_event_member
customer_event_order
customer_event_tier_change
customer_identity
delete_customer_tag
etl_pull_customer
etl_pull_customer_identity
etl_pull_customer_tag
etl_pull_tags
etl_update_customer_identity
etl_update_customer_tag
full_order.insert_customer_tag
tags
customer_event_app_messsage_clicked
customer_event_app_member_bind_event
customer_event_app_first_launch_event
customer_attr_apppush
customer_apppush
insert_app_notification_identity
delete_app_notification_identity
customer_identity_apppush
etl_pull_app_visitor
refresh_customer_extended_attr_all
customer_extended_attr_hbase

list_rebuild_trigger_queue




--值标签任务重跑
--修改hive分区(因为写入hbase任务不能直接重跑,他需要跟前一天的数据进行比较)

alter table etl_update_customer_value_tag partition(create_day=20200608) rename to partition(create_day=2020060801);
alter table etl_update_customer_value_tag partition(create_day=20200609) rename to partition(create_day=20200608);

--删除etl任务
delete from etl_task_run where period='2020-06-09' and task_name='SparkETL';

--删除需要重跑的任务
delete from etl_task_run where period='2020-06-09' and task_name in'';


每隔10分钟执行一次：0 */10 * * * ?
每隔3秒执行一次：*/3 * * * * ?
在每天23点15分执行一次：0 15 23 * * ?
在每天凌晨1点执行一次：0 0 1 * * ?
每月3号的凌晨1点执行一次：0 0 1 3 * ?
每月最后一天的0点执行一次：0 0 0 L * ?
每周星期天凌晨1点执行一次：0 0 1 ? * L
每月的最后一个星期三下午15点执行：0 0 15 ? * 4L
在26分、29分、33分执行一次：0 26,29,33 * * * ?
每天的0点、12点、18点、21点执行一次：0 0 0,12,18,21 * * ?
每天的17点到18点之间从17:05:23开始每15分钟执行一次：23 5/15 17 * * ?
