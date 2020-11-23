--------------------ETL任务监控
--查看当天所有ETL任务的状态
select task_name,status from etl_task_run where period='2020-04-15';
--export任务每天2点开始依次执行,3点执行完成
--出现任务卡死时,可以在yarn页面kill掉,ETL会自动重试
select task_name,status from etl_task_run where period='2020-04-15' and task_name like 'export%';
--load任务10点左右执行完成,出现延迟时,我们先确认s3路径没有文件,再通知operation介入
select task_name,status from etl_task_run where period='2020-04-15' and task_name like 'load%';
--任务失败时,可查看ETL日志文件定位问题,根据不同的任务定义,再重新执行一个或多个任务
update etl_task_run set status='Resume' where status='Fail' and period='2020-05-05';


--查看当天ETL任务执行日志
select * from etl_task_log where period='2020-04-15';


--------------------脏数据监控
--查看会员号身份与会员号属性不一致:
--a)多个会员号合并到同一个客户
--b1)或者kudu的客户表数据丢失(1,客户表从mysql到kudu数据没同步 2,mysql客户表数据丢失)
--b2)当前程序正在依次创建客户身份,客户和客户属性,有可能查询到创建一半的数据,可以通过last_updated时间来判断
select value as membercode,customer_id,last_updated from(select ci.value,ci.customer_id,ci.last_updated
from (select ci.*,row_number() over(partition by value order by version desc) rn from customer_identity ci where ci.tenant_id=1 and ci.type='c_membercode') ci 
left outer join customer c on c.tenant_id=1 and ci.customer_id=c.id
left outer join customer_attribute ct on ct.tenant_id=1 and c.id=ct.customer_id
where ci.rn=1 and ci.value<>nvl(ct.attr1,'')) t order by last_updated
--c)如果身份表从mysql到kudu数据没同步,无法通过上面的SQL统计到
----第二天ETL时会创建一个没有身份的客户(mysql主键冲突导致),但会同时更新该mysql身份,第三天ETL就正常了


-------------------智能群组计算监控
--所有群组重算任务完成后,查看智能群组计算失败log
"https://app.map.adidas.com.cn/elk/app/kibana#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:now-12h,mode:quick,to:now))&_a=(columns:!(message,service,x_request_id,level),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:AW0KZGqxan2dR7dMfFP-,key:service,negate:!f,params:!(customerbackend,impala-query),type:phrases,value:'customerbackend,%20impala-query'),query:(bool:(minimum_should_match:1,should:!((match_phrase:(service:customerbackend)),(match_phrase:(service:impala-query)))))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:AW0KZGqxan2dR7dMfFP-,key:level,negate:!f,type:phrase,value:ERROR),query:(match:(level:(query:ERROR,type:phrase)))),('$state':(store:appState),meta:(alias:!n,disabled:!f,index:AW0KZGqxan2dR7dMfFP-,key:message,negate:!f,params:!('preview%20compute%20query%20error','query%20impala%20failed'),type:phrases,value:'preview%20compute%20query%20error,%20query%20impala%20failed'),query:(bool:(minimum_should_match:1,should:!((match_phrase:(message:'preview%20compute%20query%20error')),(match_phrase:(message:'query%20impala%20failed'))))))),index:AW0KZGqxan2dR7dMfFP-,interval:auto,query:(match_all:()),sort:!('@timestamp',desc))"
--查看mysql群组的最大执行时间
select max(date_created) from list_member where tenant_id=1 and list_id=?;