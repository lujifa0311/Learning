('load_d301_dwm_customer','day','shell','2019-07-01 00:00:00','xiapengcheng',true,
null,
'echo 0;files1=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/d301_dwm_customer/o_date={next_date_key}/|grep 
"[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;sleep 20;
files2=`aws s3 ls --region cn-north-1 s3://bji-prd-map/output/daily/d301_dwm_customer/o_date={next_date_key}/|grep 
"[[:digit:]]\\{6\\}_[[:digit:]]\\+$"`;
if [ "$files1" = "$files2" -a -n "$files1" ];
then echo -1 ; 
hdfs --config /etc/hadoop/conf/ dfs -rm -r /user/hive/warehouse/cdp_prod.db/ods_d301_dwm_customer/create_day={date_key} ; 
echo -2 ; 
hdfs --config /etc/hadoop/conf/ dfs -mkdir /user/hive/warehouse/cdp_prod.db/ods_d301_dwm_customer/create_day={date_key} 
&& echo -3 && 
hdfs --config /etc/hadoop/conf/ dfs -cp s3a://bji-prd-map/output/daily/d301_dwm_customer/o_date={next_date_key}/* /user/hive/warehouse/cdp_prod.db/ods_d301_dwm_customer/create_day={date_key} 
&& echo 1; 
else echo -9; 
fi',
'{"startHour":"4","rerunInterval":"15","rerunTimes":"26"}',
'MSCK REPAIR TABLE ods_d301_dwm_customer;select count(1)>0 from ods_d301_dwm_customer where create_day={date_key}'
),

with ci1 as(
    select * from(
    select customer_id,type,value,row_number() over(partition by type,value order by version desc) rn from customer_identity ci1 left anti join customer_identity ci11
    on ci11.tenant_id=8 and ci11.type='c_customer_id' and ci1.customer_id=ci11.customer_id where ci1.tenant_id=8 and ci1.type in('alipay','wechat-unionid','wechat')) ci where ci.rn=1
)
insert overwrite table etl_pull_customer partition(create_day={date_key})
select
nvl(ci.customer_id,nvl(ci2.customer_id,nvl(ci3.customer_id,nvl(ci1.customer_id,cast(c.customer_id as bigint))))) as id,
ci.customer_id his_customer_id,
ci1.customer_id ali_customer_id,
ci2.customer_id wechat_customer_id,
ci3.customer_id openid_customer_id,
case when ci.customer_id is null or ci.attr3 is null then 1
    when ci.attr_cus_id is not null and nvl(ci.attr1,'')<>c.customer_id then -1
    when c.member_update_time>='{latest_etl_date_id}' then 2
    when (c.customer_type='member w trans' and ci.attr3='无交易会员') then 3
    else 0 end as update_flag,
case when (ci.customer_id is null or ci.attr3 is null) or (c.member_update_time>='{latest_etl_date_id}') or (c.customer_type='member w trans' and ci.attr3='无交易会员') then nvl(ci.version+1,0)
    else ci.version end as version,
c.client_name,
c.channel_name,
c.mobile_province_name,
c.mobile_city_name,
c.customer_key,
c.customer_id,
c.customer_name,
c.likely_member,
c.mobile_no,
c.mobile_province_cd,
c.mobile_city_cd,
c.birthday,
c.sex,
c.district_cd,
c.district_name,
c.city_cd,
c.city_name,
c.province_cd,
c.province_name,
c.address,
c.client_key,
c.data_source,
c.recruit_source,
c.member_create_time,
c.member_update_time,
c.wx_uid,
c.wx_name,
c.wxepay_uid,
c.wechatpaymcard_id,
c.wechatpaymcard_create_time,
c.wechatpay_data_source,
c.zfb_uid,
c.alipaymcard_id,
c.alipaymcard_create_time,
c.alipay_data_source,
c.tmall_create_time,
c.tmall_data_source,
c.tb_uid,
c.tb_name,
c.qq_uid,
c.qq_name,
c.tmall_uid,
c.tmall_username,
c.children_info_exist,
c.child_sex1,
c.child_birthday1,
c.child_sex2,
c.child_birthday2,
c.child_name1,
c.child_name2,
c.stature_sex,
c.stature_weight,
c.stature_height,
c.stature_hipline,
c.stature_waistline,
c.stature_leg_length,
c.stature_shoulder_width,
c.stature_thigh,
c.stature_rump_length,
c.stature_cup_upper_bust,
c.stature_cup_under_bust,
c.stature_cup_size,
c.stature_cup_length,
c.stature_neck_circumference,
c.stature_outside_sleeve,
c.marriage,
c.user_id,
c.wx_openid1,
c.wx_openid1_create_timestamp,
c.wx_openid2,
c.wx_openid2_create_timestamp,
c.wx_openid3,
c.wx_openid3_create_timestamp,
c.wx_openid4,
c.wx_openid4_create_timestamp,
c.wx_openid5,
c.wx_openid5_create_timestamp,
c.android_pushid_jg,
c.android_pushid_jg_create_timestamp,
c.ios_pushid_jg,
c.ios_pushid_jg_create_timestamp,
c.weekly_new_product,
c.weekly_limited_time_premium,
c.member_subscriptions,
c.customer_type,
c.member_status,
c.store_id_24h,
c.etl_date,
c.create_time,
c.update_time,
c.ods_create_day
from (select c.*,case when zfb_uid<>'' and zfb_uid<>'None' then zfb_uid else concat('rand',customer_key) end new_zfb_uid,
             case when wxepay_uid<>'' and wxepay_uid<>'None' then wxepay_uid when wx_uid<>'' and wx_uid<>'None' then wx_uid else concat('rand',customer_key) end new_wx_uid,
             case when wx_openid2<>'' and wx_openid2<>'None' then wx_openid2 else concat('rand',customer_key) end new_wx_openid2
             from ods_full_dim_customer c where create_day={date_key}) c
left outer join (select ci.customer_id,ci.value,ct.version,ct.attr3,ct.customer_id attr_cus_id,ct.attr1,row_number() over(partition by ci.value order by ci.version desc) rn from customer_identity ci left outer join customer_attribute ct on ct.tenant_id=8 and ci.customer_id=ct.customer_id where ci.tenant_id=8 and ci.type='c_customer_id') ci
    on c.customer_id=ci.value and ci.rn=1
left outer join ci1 on c.new_zfb_uid=ci1.value and ci1.type='alipay'
left outer join ci1 as ci2 on c.new_wx_uid=ci2.value and ci2.type='wechat-unionid'
left outer join ci1 as ci3 on c.new_wx_openid2=ci3.value and ci3.type='wechat'

