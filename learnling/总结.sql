修改表中数据注意
select 
   mt.mf,
   mt.mdate,
   c.customer_id
  from (select 
            case when mt.if_only_fror_buyer=1 then 1 else 2 end mf,
			date_format(to_utc_timestamp(mt.matf_comms_date,'Asia/Shanghai'),"yyyy-MM-dd'T'HH:mm:ss'Z'") mdate,
            mt.member_id md		
	     from ods_matf_communication_tag_monthly mt where mt.create_day=20200617) mt 
   left outer join etl_pull_customer c on c.create_day=20200617 and c.update_flag>=0 and c.member_id = mt.member_id limit 11;

1.表的优化
2.关联时数据源在前,修改数据在后,不然会修改全部的数据(关联上的修改,关联不上的修改为null)

'{"startHour":"7","rerunInterval":"15","rerunTimes":"8","rerunInSucceed":"1"}'
startHour:开始执行时间
rerunInterval:重试执行次数
rerunTimes:重试执行间隔时间
rerunInSucceed:如果已达到执行次数就返回成功

定时器
1.{minute} {hour} {day-of-month} {month} {day-of-week} {full-path-to-shell-script} 
1. minute: 区间为 0 – 59 
1. hour: 区间为0 – 23 
1. day-of-month: 区间为0 – 31 
1. month: 区间为1 – 12. 1 是1月. 12是12月. 
1. Day-of-week: 区间为0 – 7. 周日可以是0或7.

2.crontab –e : 修改 crontab 文件. 如果文件不存在会自动创建。 
2.crontab –l : 显示 crontab 文件。 
2.crontab -r : 删除 crontab 文件。
2.crontab -ir : 删除 crontab 文件前提醒用户

3.星号（*）：代表所有可能的值，例如month字段如果是星号，则表示在满足其它字段的制约条件后每月都执行该命令操作
3.逗号（,）：可以用逗号隔开的值指定一个列表范围，例如，“1,2,5,7,8,9”
3.中杠（-）：可以用整数之间的中杠表示一个整数范围，例如“2-6”表示“2,3,4,5,6”
3.正斜线（/）：可以用正斜线指定时间的间隔频率，例如“0-23/2”表示每两小时执行一次

sql where和and的区别
join后跟and的过滤条件只对右表有作用  最后结果还是返回左表
join后跟where是对全表的条件进行过滤

2020.07.06 问题解决
1.对数问题  ods_matf_communication_tag_monthly关联etl_pull_customer修改customer_attrbute其中的attr10 和attr18 的属性值
在对数过程中发现已修改完的数据和匹配不上的数据不一致,通过多次查询 发现数据还在发生变化   
归结原因:有程序在修改attr8的字段  所以导致数据不一致
修改方案:把attr8的字段换成了attr10

2020.07.09 
问题:关于个别task在某一台节点上卡死
解决方法:ssh到任务卡死的节点上 kill -9 CDGD
         然后卡死的任务就会跑到其他节点上继续运行
		 
2020.07.09 
值标签对数 给一个值标签的ID 来对数
select count(1) from etl_pull_customer_value_tag,etl_pull_customer_value_tag.tags as tags where key='10603'and create_day=20200710;






