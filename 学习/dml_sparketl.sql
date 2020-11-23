insert into xiaoshu.etl_task_schedule(task_name, exec_period, task_type,create_time, author, is_valid, pre_condition, exec_sql, exec_params, check_sql)values
('load_ods_198_pd_as_user','day','sql','2019-07-01 00:00:00','lujifa',true,
null,
'insert overwrite table ods_198_pd_as_user partition(create_day=20200820)
select
id,
name,
nick_name,
sex,
birthday,
cell_phone,
cell_phone_verified,
email,
email_verified,
password_md5,
payment_password,
qq,
header_url,
status,
source,
is_loyalty,
loyalty_code,
loyalty_level,
join_loyalty_store_code,
join_loyalty_store_name,
join_loyalty_chanel,
join_loyalty_time,
become_vip_time,
last_change_level_time,
over_vip_time,
consume,
open_id,
union_id,
userful_point,
keep_vip_day,
become_vip_need_point,
coming_over_point,
creator,
create_time,
modifier,
modify_time,
deleted,
as_id,
assessment_date,
upgrade_gifts,
last_level,
join_city,
join_city_code,
refer_cell_phone,
login_coupon_logo,
anonymous_flag,
LoyaltyID,
LoyaltyID2,
LoyaltyID3,
upgrade_gifts_need_pionts,
family_size,
baby_number,
is_send_loyalty_gift
from as_user',
null,
null
);


insert into xiaoshu.etl_task_schedule(task_name, exec_period, task_type,create_time, author, is_valid, pre_condition, exec_sql, exec_params, check_sql)values
('etl_pull_customer','day','sql','2019-07-01 00:00:00','lujifa',true,
'select count(1)=0 from etl_task_run where task_name in(''load_ods_198_pd_as_user'') and period=''{date_id}'' and status=''Succeed''',
'with ci1 as(
    select * from(
    select customer_id,type,value,row_number() over(partition by type,value order by version desc) rn from customer_identity ci1 left anti join customer_identity ci11
    on ci11.tenant_id=1 and ci11.type=''as_userid'' and ci1.customer_id=ci11.customer_id where ci1.tenant_id=1 and ci1.type in(''wechat-unionid'',''wechat'',''memberId'')) ci where ci.rn=1
)
insert overwrite table etl_pull_customer partition(create_day={date_key})
select
nvl(ci.customer_id,nvl(ci3.customer_id,nvl(ci2.customer_id,nvl(ci1.customer_id,cast(concat(''1'',c.id) as bigint))))) as id,
ci.customer_id his_customer_id,
ci1.customer_id open_customer_id,
ci2.customer_id wechat_customer_id,
ci3.customer_id member_customer_id,
case when ci.customer_id is null or c.create_time=c.modify_time then 1
     when c.create_time<>c.modify_time then 2
     when nvl(ci.attr1,'''')<>c.id then -1
	 else 0 end as update_flag,
case when ci.customer_id is null or c.create_time=c.modify_time then 0 end version,
c.id,
c.name,
c.nick_name,
c.sex,
c.birthday,
c.cell_phone,
c.cell_phone_verified,
c.email,
c.email_verified,
c.password_md5,
c.payment_password,
c.qq,
c.header_url,
c.status,
c.source,
c.is_loyalty,
c.loyalty_code,
c.loyalty_level,
c.join_loyalty_store_code,
c.join_loyalty_store_name,
c.join_loyalty_chanel,
c.join_loyalty_time,
c.become_vip_time,
c.last_change_level_time,
c.over_vip_time,
c.consume,
c.open_id,
c.union_id,
c.userful_point,
c.keep_vip_day,
c.become_vip_need_point,
c.coming_over_point,
c.creator,
c.create_time,
c.modifier,
c.modify_time,
c.deleted,
c.as_id,
c.assessment_date,
c.upgrade_gifts,
c.last_level,
c.join_city,
c.join_city_code,
c.refer_cell_phone,
c.login_coupon_logo,
c.anonymous_flag,
c.LoyaltyID,
c.LoyaltyID2,
c.LoyaltyID3,
c.upgrade_gifts_need_pionts,
c.family_size,
c.baby_number,
c.is_send_loyalty_gift
from (select c.*,case when open_id<>'''' then open_id else concat(''rand'',id) end new_wx_openid,
                 case when union_id<>'''' then union_id else concat(''rand'',id) end new_wx_unionid,
                 case when loyalty_code<>'''' then loyalty_code else concat(''rand'',id) end new_memberid
             from ods_198_pd_as_user c where create_day={date_key} and deleted=0) c
left outer join (select ci.customer_id,ci.value,ct.version,ct.attr1,row_number() over(partition by ci.value order by ci.version desc) rn from customer_identity ci left outer join customer_attribute ct on ct.tenant_id=1 and ci.customer_id=ct.customer_id where ci.tenant_id=1 and ci.type=''as_userid'') ci
    on c.id=ci.value and ci.rn=1
left outer join ci1 on c.new_wx_openid=ci1.value and ci1.type=''wechat''
left outer join ci1 as ci2 on c.new_wx_unionid=ci2.value and ci2.type=''wechat-unionid''
left outer join ci1 as ci3 on c.new_memberid=ci3.value and ci3.type=''memberId''',
null,
null
),


--'select count(1)=0 from etl_task_run where task_name in(''etl_pull_customer'') and period=''{date_id}'' and status=''Succeed'''
insert into xiaoshu.etl_task_schedule(task_name, exec_period, task_type,create_time, author, is_valid, pre_condition, exec_sql, exec_params, check_sql)values
('customer_identity','day','write_mysql','2019-07-01 00:00:00','lujifa',true,
null,
'',
'{"sql":"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;insert into xiaoshu.customer_identity(tenant_id,version,type,value,customer_id,date_created,last_updated) values (1,0,?,?,?,?,now())"}',
null
),

insert into xiaoshu.etl_task_schedule(task_name, exec_period, task_type,create_time, author, is_valid, pre_condition, exec_sql, exec_params, check_sql)values
('customer','day','write_mysql','2019-07-01 00:00:00','lujifa',true,
null,
'select 
id,
name,
cast(sex as int) gender,
from_unixtime(unix_timestamp(birthday,''yyyy-MM-dd''),''yyyy-MM-dd'') birthday,
case when length(cell_phone)=11 and cell_phone<>''10000000000'' and cell_phone not like ''10%'' and cell_phone not like ''11%'' and cell_phone not like ''12%'' then cell_phone else null end  mobile,
nvl(cast(cell_phone_verified as int),0) mobile_verified,
name wechat_nick_name,
null country,
null province,
null city,
null county,
email,
nvl(cast(email_verified as int),0) email_verified,
qq,
to_utc_timestamp(create_time,''Asia/Shanghai'') date_join,
null create_from_name,
null create_method,
case when join_loyalty_chanel is null then ''Other'' else join_loyalty_chanel end source,
nvl(cast(is_loyalty as int),0) is_member,
header_url img,
to_utc_timestamp(create_time,''Asia/Shanghai'') date_created
from etl_pull_customer where create_day={date_key} and update_flag>=0',
'{"sql":"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;insert into xiaoshu.customer(tenant_id,version,id,name,gender,birthday,mobile,mobile_verified,wechat_nick_name,country,province,city,county,email,email_verified,qq,date_join,create_from_name,create_method,source,is_member,img,date_created,last_updated) values (1,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now()) ON DUPLICATE KEY UPDATE tenant_id=1,name=values(name),gender=values(gender),birthday=values(birthday),mobile=values(mobile),mobile_verified=values(mobile_verified),wechat_nick_name=values(wechat_nick_name),countsry=values(country),province=values(province),city=values(city),county=values(county),email=values(email),email_verified=values(email_verified),qq=values(qq),date_join=values(date_join),is_member=values(is_member),img=values(img),date_created=values(date_created),last_updated=now()"}',
null
),



insert into xiaoshu.etl_task_schedule(task_name, exec_period, task_type,create_time, author, is_valid, pre_condition, exec_sql, exec_params, check_sql)values
('customer_attribute','day','write_mysql','2019-07-01 00:00:00','lujifa',true,
null,
'select 
id,
as_userid attr1,
nvl(loyalty_code,'''') attr2,
case when join_loyalty_chanel is null then ''Other'' else join_loyalty_chanel end attr3,
nvl(join_loyalty_store_code,'''') attr4,
nvl(join_loyalty_store_name,'''') attr5,
nvl(become_vip_need_point,'''') attr6,
case when status = ''0'' then ''正常'' when status = ''1'' then ''禁用'' else ''冻结'' end attr7,
loyalty_level attr8,
nvl(date_format(to_utc_timestamp(join_loyalty_time,''Asia/Shanghai''),"yyyy-MM-dd''T''HH:mm:ss''Z''"),'''') attr9,
nvl(date_format(to_utc_timestamp(become_vip_time,''Asia/Shanghai''),"yyyy-MM-dd''T''HH:mm:ss''Z''"),'''') attr10,
nvl(date_format(to_utc_timestamp(last_change_level_time,''Asia/Shanghai''),"yyyy-MM-dd''T''HH:mm:ss''Z''"),'''') attr11,
nvl(date_format(to_utc_timestamp(over_vip_time,''Asia/Shanghai''),"yyyy-MM-dd''T''HH:mm:ss''Z''"),'''') attr12,
case when consume = ''1'' then ''新会员'' when consume = ''2'' then ''成长会员'' when consume = ''3'' then ''成熟会员'' when consume = ''4'' then ''衰退会员'' when consume = ''5'' then ''流失会员'' else null end attr13,
userful_point attr14,
keep_vip_day attr15,
coming_over_point attr16,
date_format(to_utc_timestamp(assessment_date,''Asia/Shanghai''),"yyyy-MM-dd''T''HH:mm:ss''Z''") attr17,
case when upgrade_gifts = ''1'' then ''已拿到'' else ''未拿到'' end attr18,
case when last_level = ''1'' then ''新晋'' when last_level = ''2'' then ''资深'' when last_level = ''3'' then ''终极'' else null end attr19,
join_city attr20,
join_city_code attr21,
refer_cell_phone attr22,
case when login_coupon_logo = ''1'' then ''已领取'' else ''未领取'' end attr23,
null attr24,
null attr25,
null attr26,
null attr27,
null attr28,
case when is_send_loyalty_gift = ''1'' then ''是'' else ''否'' end attr29,
to_utc_timestamp(create_time,''Asia/Shanghai'') date_created
from etl_pull_customer where create_day={date_key} and update_flag>=0',
'{"sql":"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;insert into xiaoshu.customer_attribute(tenant_id,version,customer_id,attr1,attr2,attr3,attr4,attr5,attr6,attr7,attr8,attr9,attr10,attr11,attr12,attr13,attr14,attr15,attr16,attr17,attr18,attr19,attr20,attr21,attr22,attr23,attr24,attr25,attr26,attr27,attr28,attr29,date_created,last_updated)values (1,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now()) ON DUPLICATE KEY UPDATE attr1=values(attr1),attr2=values(attr2),attr3=values(attr3),attr4=values(attr4),attr5=values(attr5),attr6=values(attr6),attr7=values(attr7),attr8=values(attr8),attr9=values(attr9),attr10=values(attr10),attr11=values(attr11),attr12=values(attr12),attr13=values(attr13),attr14=values(attr14),attr15=values(attr15),attr16=values(attr16),attr17=values(attr17),attr18=values(attr18),attr19=values(attr19),attr20=values(attr20),attr21=values(attr21),attr22=values(attr22),attr23=values(attr23),attr24=values(attr24),attr25=values(attr25),attr26=values(attr26),attr27=values(attr27),attr28=values(attr28),attr29=values(attr29),date_created=values(date_created),last_updated=now()"}',
null
),
