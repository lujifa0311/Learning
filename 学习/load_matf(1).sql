--新增load_matf_communication_tag_monthly,update_matf_communication_tag_monthly任务
--导入数据
('load_matf_communication_tag_monthly','day','shell','2019-07-01 00:00:00','xiapengcheng',true,
null,
'echo 0;files1=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/f336_matf_communication_tag_monthly/o_date={next_date_key}/|grep "[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;sleep 20;files2=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/f336_matf_communication_tag_monthly/o_date={next_date_key}/|grep "[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;if [ "$files1" = "$files2" -a -n "$files1" ];then echo -1 ; hdfs --config /etc/hadoop/conf/ dfs -rm -r /user/hive/warehouse/cdp_prod.db/ods_matf_communication_tag_monthly/create_day={date_key} ; echo -2 ; hdfs --config /etc/hadoop/conf/ dfs -mkdir /user/hive/warehouse/cdp_prod.db/ods_matf_communication_tag_monthly/create_day={date_key} && echo -3 && hdfs --config /etc/hadoop/conf/ dfs -cp s3a://bji-prd-map/output/daily/f336_matf_communication_tag_monthly/o_date={next_date_key}/* /user/hive/warehouse/cdp_prod.db/ods_matf_communication_tag_monthly/create_day={date_key} && echo 1; else echo -9; fi',
'{"startHour":"7","rerunInterval":"15","rerunTimes":"8","rerunInSucceed":"1"}',
'MSCK REPAIR TABLE ods_matf_communication_tag_monthly;select count(1)>0 from ods_matf_communication_tag_monthly where create_day={date_key}'
),

-- 关联member_id
('update_matf_communication_tag_monthly','day','write_mysql','2019-07-01 00:00:00','xiapengcheng',true,
'select count(1)=2 from etl_task_run where task_name in(''etl_pull_customer'',''ods_matf_communication_tag_monthly'') and period=''{date_id}'' and status=''Succeed''',
'select 
   mt.mf,
   mt.mdate,
   c.id
  from (select 
            case when mt.if_only_fror_buyer=''1'' then ''是'' else ''否'' end mf,
            date_format(to_utc_timestamp(mt.matf_comms_date,'Asia/Shanghai'),"yyyy-MM-dd'T'HH:mm:ss'Z'") mdate,
            mt.member_id
        from ods_matf_communication_tag_monthly mt where mt.create_day={date_key}) mt 
   left outer join etl_pull_customer c on c.create_day={date_key} and c.update_flag>=0 and c.member_id = mt.member_id',
'{"sql":"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;update xiaoshu.customer_attribute set attr8=?,attr18=? where tenant_id=1 and customer_id=?"}',
null
)
--新增load_matf_communication_tag_monthly,update_matf_communication_tag_monthly任务 

else if [ "$files1" = "$files2" ];then echo 2; else echo -9;fi; fi',
'{"startHour":"4","rerunInterval":"15","rerunTimes":"20","rerunInSucceed":"1"}', 


('load_matf_communication_tag_monthly','day','shell','2019-07-01 00:00:00','xiapengcheng',true,
null,
'echo 0;files1=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/f336_matf_communication_tag_monthly/o_date={next_date_key}/|grep "[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;sleep 20;files2=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/f336_matf_communication_tag_monthly/o_date={next_date_key}/|grep "[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;if [ "$files1" = "$files2" -a -n "$files1" ];then echo -1 ; hdfs --config /etc/hadoop/conf/ dfs -rm -r /user/hive/warehouse/cdp_prod.db/ods_matf_communication_tag_monthly/create_day={date_key} ; echo -2 ; hdfs --config /etc/hadoop/conf/ dfs -mkdir /user/hive/warehouse/cdp_prod.db/ods_matf_communication_tag_monthly/create_day={date_key} && echo -3 && hdfs --config /etc/hadoop/conf/ dfs -cp s3a://bji-prd-map/output/daily/f336_matf_communication_tag_monthly/o_date={next_date_key}/* /user/hive/warehouse/cdp_prod.db/ods_matf_communication_tag_monthly/create_day={date_key} && echo 1; else if [ "$files1" = "$files2" ];then echo 2; else echo -9;fi; fi',
'{"startHour":"7","rerunInterval":"15","rerunTimes":"8","rerunInSucceed":"1"}',
'MSCK REPAIR TABLE ods_matf_communication_tag_monthly;select count(1)>0 from ods_matf_communication_tag_monthly where create_day={date_key}'
),     
 
--测试数据 	 
select 
   mt.mf,
   mt.mdate,
   c.id
  from (select 
            case when mt.if_only_fror_buyer='1' then '是' else '否' end mf,
            date_format(to_utc_timestamp(mt.matf_comms_date,'Asia/Shanghai'),"yyyy-MM-dd'T'HH:mm:ss'Z'") mdate,
            mt.member_id
        from ods_matf_communication_tag_monthly mt where mt.create_day=20200617) mt 
   left outer join etl_pull_customer c on c.create_day=20200617 and c.update_flag>=0 and c.member_id = mt.member_id;
        


  update xiaoshu.customer_attribute set attr8=?,attr18=? where tenant_id=1 and customer_id=?;