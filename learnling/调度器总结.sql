--插入任务定义
insert into xiaoshu.etl_task_schedule(task_name, exec_period, task_type,create_time, author, is_valid, pre_condition, exec_sql, exec_params, check_sql)values
('load_d211_dnm_couponrule',--任务名称
'day',--执行频率
'shell',--任务类型
'2019-07-01 00:00:00',--执行时间
'xiapengcheng',--
true,--任务依赖
null,
'echo 0;files1=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/d211_dnm_couponrule/o_date={next_date_key}/|grep "[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;sleep 20;files2=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/d211_dnm_couponrule/o_date={next_date_key}/|grep "[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;if [ "$files1" = "$files2" -a -n "$files1" ];then echo -1 ; hdfs --config /etc/hadoop/conf/ dfs -rm -r /user/hive/warehouse/cdp_prod.db/ods_d211_dnm_couponrule/create_day={date_key} ; echo -2 ; hdfs --config /etc/hadoop/conf/ dfs -mkdir /user/hive/warehouse/cdp_prod.db/ods_d211_dnm_couponrule/create_day={date_key} && echo -3 && hdfs --config /etc/hadoop/conf/ dfs -cp s3a://bji-prd-map/output/daily/d211_dnm_couponrule/o_date={next_date_key}/* /user/hive/warehouse/cdp_prod.db/ods_d211_dnm_couponrule/create_day={date_key} && echo 1; else if [ "$files1" = "$files2" ];then echo 2; else echo -9;fi; fi',
--判断数据是否存在
'{"startHour":"4","rerunInterval":"15","rerunTimes":"20","rerunInSucceed":"1"}',
--调度参数
--startHour:开始执行时间
--rerunInterval:重试执行次数
--rerunTimes:重试执行间隔时间
--rerunInSucceed:如果已达到执行次数就返回成功
'MSCK REPAIR TABLE ods_d211_dnm_couponrule;select count(1)>0 from ods_d211_dnm_couponrule where create_day={date_key}'
--刷新源数据  检查数据
),

('customer_identity',
'day',
'write_mysql',
'2019-07-01 00:00:00',
'xiapengcheng',
true,
'select count(1)=3 from etl_task_run where task_name in(''etl_pull_customer'',''etl_pull_customer_identity'',''customer_attr'') and period=''{date_id}'' and status=''Succeed''',
'select ci.* from (
select ci.type,ci.value,c.id,to_utc_timestamp(ci.state_date,''Asia/Shanghai'') date_created from etl_pull_customer_identity ci inner join etl_pull_customer c on c.create_day={date_key} and c.update_flag>=0 and ci.member_id=c.member_id and ci.create_day={date_key} and ci.identity_customer_id is null
union all
select ''c_membercode'',MEMBER_CODE,id,to_utc_timestamp(REGIST_DATE,''Asia/Shanghai'') from etl_pull_customer where create_day={date_key} and update_flag=1
) ci left anti join customer_identity ci1 on ci1.tenant_id=1 and ci.type=ci1.type and ci.value=ci1.value',
'{"sql":"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;insert into xiaoshu.customer_identity(tenant_id,version,type,value,customer_id,date_created,last_updated) values (1,0,?,?,?,?,now()) ON DUPLICATE KEY UPDATE version=version+1"}',
null
)

--查询

--查询任务运行状态
select task_name,status,task_type,last_updated from etl_task_run where period='2020-07-01';
--查询任务运行日志
select * from etl_task_log where period='2020-07-01';
--查询任务定义
select * from etl_task_schedule where task_name='';
--修改任务任务状态
update etl_task_run set status='Resume' where status='Fail' and period='2020-07-02';
--删除已有任务定义
delete from  xiaoshu.etl_task_schedule where task_name='update_matf_communication_tag_monthly';
--删除run中的任务
delete from xiaoshu.etl_task_run where period='2020-07-01' and task_name='SparkETL';










