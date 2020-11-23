--首先查看aws上的文件是否正确(与昨天进行比较)


--查看hive上文件是否正确


--统计ods表中的数据量查看数据是否正确


--值标签任务重跑
--修改hive分区(因为写入hbase任务不能直接重跑,他需要跟前一天的数据进行比较)
alter table ods_198_pd_ordcussrvdetail_extension partition(create_day=20200925) rename to partition(create_day=2020092503);
alter table etl_update_customer_value_tag partition(create_day=20200609) rename to partition(create_day=20200608);

--删除etl任务
delete from etl_task_run where period='2020-06-09' and task_name='SparkETL';

--删除需要重跑的任务
delete from etl_task_run where period='2020-06-09' and task_name in'';






alter table ods_198_pd_usercoupon partition(create_day=20200924) rename to partition(create_day=20200925);