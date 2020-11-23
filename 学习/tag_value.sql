10876  attr21  用户产品推荐|WMS|Top 1 Search recommendation article
10964  attr23  用户产品推荐|WMS|Size check GYL product
10965  attr26  用户产品推荐|WMS|Cancel order GYL product
10880  attr27  用户产品推荐|APP|Top 1 Search recommendation article
10966  attr30  用户产品推荐|APP|Size check GYL product
10967  attr31  用户产品推荐|APP|Cancel order GYL product
10884  attr32  用户产品推荐|.COM|Top 1 Search recommendation article
10968  attr33  用户产品推荐|.COM|Size check GYL product
10969  attr38  用户产品推荐|.COM|Cancel order GYL product

select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id in ('cdp_10876','cdp_10964','cdp_10965','cdp_10880','cdp_10966','cdp_10967','cdp_10884','cdp_10968','cdp_10969');


attr21 10876   | text204 |  attr21
attr27 10880   | text206 |  attr27  
attr32 10884   | text205 |  attr32
attr23 10964   | text210 |  attr23
attr26 10965   | text208 |  attr26
attr30 10966   | text211 |  attr30
attr31 10967   | text212 |  attr31
attr33 10968   | text207 |  attr33
attr38 10969   | text209 |  attr38

case when tags.text204.attr_value<>'' then tags.text204.attr_value else default_article['WMS'] end attr21, 
case when tags.text206.attr_value<>'' then tags.text206.attr_value else default_article['WMS'] end attr27, 
case when tags.text205.attr_value<>'' then tags.text205.attr_value else default_article['WMS'] end attr32, 
case when tags.text210.attr_value<>'' then tags.text210.attr_value else default_article['WMS'] end attr23, 
case when tags.text208.attr_value<>'' then tags.text208.attr_value else default_article['WMS'] end attr26, 
case when tags.text211.attr_value<>'' then tags.text211.attr_value else default_article['WMS'] end attr30, 
case when tags.text212.attr_value<>'' then tags.text212.attr_value else default_article['WMS'] end attr31, 
case when tags.text207.attr_value<>'' then tags.text207.attr_value else default_article['WMS'] end attr33, 
case when tags.text209.attr_value<>'' then tags.text209.attr_value else default_article['WMS'] end attr38 

v.attr21=ct.attr21 and v.attr27=ct.attr27 and v.attr32=ct.attr32 and v.attr23=ct.attr23 and v.attr26=ct.attr26 and v.attr30=ct.attr30 
and v.attr31=ct.attr31 and v.attr33=ct.attr33 and v.attr38=ct.attr38

v.attr21,v.attr27,v.attr32,v.attr23,v.attr26,v.attr30,v.attr31,v.attr33,v.attr38

attr21=?,attr27=?,attr32=?,attr23=?,attr26=?,attr30=?,attr31=?,attr33=?,attr38=?
---------------
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10876';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10878';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10879';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10881';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10882';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10883';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10885';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10886';
select storage_type,storage_index from customer_extended_attr_definition where tenant_id=1 and external_id='cdp_10887';

select ct.customer_id,cv.member_code from customer_extended_attr_text ct 
    left anti join customer c on ct.customer_id=c.id and c.tenant_id=1 
   left outer join etl_pull_customer_value_tag cv on cast(ct.customer_id AS String)=cv.customer_id
where tenant_id=1 and text198<>'';



insert overwrite table etl_update_customer_value_tag_attribute partition(create_day={date_key})
select ct.id attribute_id,
case when ct.id is null then -1
when (v.attr57=ct.attr57 and v.attr58=ct.attr58 and v.attr59=ct.attr59 and v.attr60=ct.attr60 and v.attr61=ct.attr61 and 
    v.attr62=ct.attr62 and v.attr63=ct.attr63 and v.attr64=ct.attr64 and v.attr65=ct.attr65 and v.attr66=ct.attr66 and 
    v.attr67=ct.attr67 and v.attr68=ct.attr68 and v.attr69=ct.attr69 and v.attr70=ct.attr70 and v.attr71=ct.attr71 and
	v.attr73=ct.attr73 and v.attr74=ct.attr74 and v.attr75=ct.attr75 and v.attr76=ct.attr76 and v.attr77=ct.attr77 and
	v.attr78=ct.attr78 and v.attr79=ct.attr79 and v.attr80=ct.attr80 and v.attr81=ct.attr81 ) then 0
else 1 end update_flag,
v.*
from (select v.customer_id,
case when tags.text132.attr_value<>'' then tags.text132.attr_value else default_article['WMS'] end attr57, 
case when tags.text126.attr_value<>'' then tags.text126.attr_value else default_article['WMS'] end attr58, 
case when tags.text133.attr_value<>'' then tags.text133.attr_value else default_article['WMS'] end attr59, 
case when tags.text124.attr_value<>'' then tags.text124.attr_value else default_article['WMS'] end attr60, 
case when tags.text129.attr_value<>'' then tags.text129.attr_value else default_article['WMS'] end attr61, 
case when tags.text136.attr_value<>'' then tags.text136.attr_value else default_article['APP'] end attr62, 
case when tags.text130.attr_value<>'' then tags.text130.attr_value else default_article['APP'] end attr63, 
case when tags.text125.attr_value<>'' then tags.text125.attr_value else default_article['APP'] end attr64, 
case when tags.text128.attr_value<>'' then tags.text128.attr_value else default_article['APP'] end attr65, 
case when tags.text131.attr_value<>'' then tags.text131.attr_value else default_article['APP'] end attr66, 
case when tags.text134.attr_value<>'' then tags.text134.attr_value else default_article['COM'] end attr67, 
case when tags.text127.attr_value<>'' then tags.text127.attr_value else default_article['COM'] end attr68, 
case when tags.text138.attr_value<>'' then tags.text138.attr_value else default_article['COM'] end attr69, 
case when tags.text135.attr_value<>'' then tags.text135.attr_value else default_article['COM'] end attr70, 
case when tags.text137.attr_value<>'' then tags.text137.attr_value else default_article['COM'] end attr71,
case when tags.text198.attr_value<>'' then tags.text198.attr_value else default_article['WMS'] end attr73,
case when tags.text193.attr_value<>'' then tags.text193.attr_value else default_article['WMS'] end attr74,
case when tags.text199.attr_value<>'' then tags.text199.attr_value else default_article['WMS'] end attr75,
case when tags.text196.attr_value<>'' then tags.text196.attr_value else default_article['APP'] end attr76,
case when tags.text197.attr_value<>'' then tags.text197.attr_value else default_article['APP'] end attr77,
case when tags.text195.attr_value<>'' then tags.text195.attr_value else default_article['APP'] end attr78,
case when tags.text192.attr_value<>'' then tags.text192.attr_value else default_article['COM'] end attr79,
case when tags.text194.attr_value<>'' then tags.text194.attr_value else default_article['COM'] end attr80,
case when tags.text200.attr_value<>'' then tags.text200.attr_value else default_article['COM'] end attr81
from etl_update_customer_value_tag v 
cross join (select map_from_entries(collect_set(struct(platform,article_code))) default_article from ods_f1104_pre_hot_click where create_day={date_key} and seq_no_cdp=1) h
where v.create_day={date_key}) v
left outer join customer_attribute ct on ct.tenant_id=1 and v.customer_id=ct.customer_id

select v.attr57,v.attr58,v.attr59,v.attr60,v.attr61,v.attr62,v.attr63,v.attr64,v.attr65,v.attr66,v.attr67,v.attr68,v.attr69,v.attr70,v.attr71,,attr73,attr74,attr75,attr76,attr77,attr78,attr79,attr80,attr81,v.attribute_id
from etl_update_customer_value_tag_attribute v
where v.create_day={date_key} and v.update_flag=1

update xiaoshu.customer_attribute set attr57=?,attr58=?,attr59=?,attr60=?,attr61=?,attr62=?,attr63=?,attr64=?,attr65=?,attr66=?,attr67=?,attr68=?,attr69=?,attr70=?,attr71=?,attr73=?,attr74=?,attr75=?,attr76=?,attr77=?,attr78=?,attr79=?,attr80=?,attr81=? where id=?




('etl_update_customer_value_tag_attribute','day','sql','2019-07-01 00:00:00','xiapengcheng',true,
'select count(1)=2 from etl_task_run where task_name in(''etl_update_customer_value_tag'',''load_f1104_pre_hot_click'') and period=''{date_id}'' and status=''Succeed''',
'insert overwrite table etl_update_customer_value_tag_attribute partition(create_day={date_key})
select ct.id attribute_id,
case when ct.id is null then -1
when (v.attr57=ct.attr57 and v.attr58=ct.attr58 and v.attr59=ct.attr59 and v.attr60=ct.attr60 and v.attr61=ct.attr61 and 
    v.attr62=ct.attr62 and v.attr63=ct.attr63 and v.attr64=ct.attr64 and v.attr65=ct.attr65 and v.attr66=ct.attr66 and 
    v.attr67=ct.attr67 and v.attr68=ct.attr68 and v.attr69=ct.attr69 and v.attr70=ct.attr70 and v.attr71=ct.attr71 and
	v.attr73=ct.attr73 and v.attr74=ct.attr74 and v.attr75=ct.attr75 and v.attr76=ct.attr76 and v.attr77=ct.attr77 and
	v.attr78=ct.attr78 and v.attr79=ct.attr79 and v.attr80=ct.attr80 and v.attr81=ct.attr81 ) then 0
else 1 end update_flag,
v.*
from (select v.customer_id,
case when tags.text132.attr_value<>'' then tags.text132.attr_value else default_article['WMS'] end attr57, 
case when tags.text126.attr_value<>'' then tags.text126.attr_value else default_article['WMS'] end attr58, 
case when tags.text133.attr_value<>'' then tags.text133.attr_value else default_article['WMS'] end attr59, 
case when tags.text124.attr_value<>'' then tags.text124.attr_value else default_article['WMS'] end attr60, 
case when tags.text129.attr_value<>'' then tags.text129.attr_value else default_article['WMS'] end attr61, 
case when tags.text136.attr_value<>'' then tags.text136.attr_value else default_article['APP'] end attr62, 
case when tags.text130.attr_value<>'' then tags.text130.attr_value else default_article['APP'] end attr63, 
case when tags.text125.attr_value<>'' then tags.text125.attr_value else default_article['APP'] end attr64, 
case when tags.text128.attr_value<>'' then tags.text128.attr_value else default_article['APP'] end attr65, 
case when tags.text131.attr_value<>'' then tags.text131.attr_value else default_article['APP'] end attr66, 
case when tags.text134.attr_value<>'' then tags.text134.attr_value else default_article['COM'] end attr67, 
case when tags.text127.attr_value<>'' then tags.text127.attr_value else default_article['COM'] end attr68, 
case when tags.text138.attr_value<>'' then tags.text138.attr_value else default_article['COM'] end attr69, 
case when tags.text135.attr_value<>'' then tags.text135.attr_value else default_article['COM'] end attr70, 
case when tags.text137.attr_value<>'' then tags.text137.attr_value else default_article['COM'] end attr71,
case when tags.text198.attr_value<>'' then tags.text198.attr_value else default_article['WMS'] end attr73,
case when tags.text193.attr_value<>'' then tags.text193.attr_value else default_article['WMS'] end attr74,
case when tags.text199.attr_value<>'' then tags.text199.attr_value else default_article['WMS'] end attr75,
case when tags.text196.attr_value<>'' then tags.text196.attr_value else default_article['APP'] end attr76,
case when tags.text197.attr_value<>'' then tags.text197.attr_value else default_article['APP'] end attr77,
case when tags.text195.attr_value<>'' then tags.text195.attr_value else default_article['APP'] end attr78,
case when tags.text192.attr_value<>'' then tags.text192.attr_value else default_article['COM'] end attr79,
case when tags.text194.attr_value<>'' then tags.text194.attr_value else default_article['COM'] end attr80,
case when tags.text200.attr_value<>'' then tags.text200.attr_value else default_article['COM'] end attr81
from etl_update_customer_value_tag v 
cross join (select map_from_entries(collect_set(struct(platform,article_code))) default_article from ods_f1104_pre_hot_click where create_day={date_key} and seq_no_cdp=1) h
where v.create_day={date_key}) v
left outer join customer_attribute ct on ct.tenant_id=1 and v.customer_id=ct.customer_id',
null,
null
),


('update_customer_value_tag_attribute','day','write_mysql','2019-07-01 00:00:00','xiapengcheng',true,
'select count(1)=1 from etl_task_run where task_name in(''etl_update_customer_value_tag_attribute'') and period=''{date_id}'' and status=''Succeed''',
'select v.attr57,v.attr58,v.attr59,v.attr60,v.attr61,v.attr62,v.attr63,v.attr64,v.attr65,v.attr66,v.attr67,v.attr68,v.attr69,v.attr70,v.attr71,v.attribute_id
from etl_update_customer_value_tag_attribute v
where v.create_day={date_key} and v.update_flag=1',
'{"sql":"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;update xiaoshu.customer_attribute set attr57=?,attr58=?,attr59=?,attr60=?,attr61=?,attr62=?,attr63=?,attr64=?,attr65=?,attr66=?,attr67=?,attr68=?,attr69=?,attr70=?,attr71=? where id=?"}',
null
),



('etl_update_customer_value_tag_attribute','day','sql','2019-07-01 00:00:00','xiapengcheng',true,
'select count(1)=2 from etl_task_run where task_name in(''etl_update_customer_value_tag'',''load_f1103_pre_hot_sales'') and period=''{date_id}'' and status=''Succeed''',
'insert overwrite table etl_update_customer_value_tag_attribute partition(create_day={date_key})
select ct.id attribute_id,
case when ct.id is null then -1
when (v.attr57=ct.attr57 and v.attr58=ct.attr58 and v.attr59=ct.attr59 and v.attr60=ct.attr60 and v.attr61=ct.attr61 and 
    v.attr62=ct.attr62 and v.attr63=ct.attr63 and v.attr64=ct.attr64 and v.attr65=ct.attr65 and v.attr66=ct.attr66 and 
    v.attr67=ct.attr67 and v.attr68=ct.attr68 and v.attr69=ct.attr69 and v.attr70=ct.attr70 and v.attr71=ct.attr71 and
    v.attr73=ct.attr73 and v.attr74=ct.attr74 and v.attr75=ct.attr75 and v.attr76=ct.attr76 and v.attr77=ct.attr77 and 
    v.attr78=ct.attr78 and v.attr79=ct.attr79 and v.attr80=ct.attr80 and v.attr81=ct.attr81 and v.attr82=ct.attr82 and 
	v.attr83=ct.attr83 and v.attr21=ct.attr21 and v.attr27=ct.attr27 and v.attr32=ct.attr32 and v.attr23=ct.attr23 and 
	v.attr26=ct.attr26 and v.attr30=ct.attr30 and v.attr31=ct.attr31 and v.attr33=ct.attr33 and v.attr38=ct.attr38) then 0
else 1 end update_flag,
v.*
from (select v.customer_id,
case when tags.text132.attr_value<>'' then tags.text132.attr_value else default_article['WMS'] end attr57, 
case when tags.text126.attr_value<>'' then tags.text126.attr_value else default_article['WMS'] end attr58, 
case when tags.text133.attr_value<>'' then tags.text133.attr_value else default_article['WMS'] end attr59, 
case when tags.text124.attr_value<>'' then tags.text124.attr_value else default_article['WMS'] end attr60, 
case when tags.text129.attr_value<>'' then tags.text129.attr_value else default_article['WMS'] end attr61, 
case when tags.text136.attr_value<>'' then tags.text136.attr_value else default_article['APP'] end attr62, 
case when tags.text130.attr_value<>'' then tags.text130.attr_value else default_article['APP'] end attr63, 
case when tags.text125.attr_value<>'' then tags.text125.attr_value else default_article['APP'] end attr64, 
case when tags.text128.attr_value<>'' then tags.text128.attr_value else default_article['APP'] end attr65, 
case when tags.text131.attr_value<>'' then tags.text131.attr_value else default_article['APP'] end attr66, 
case when tags.text134.attr_value<>'' then tags.text134.attr_value else default_article['COM'] end attr67, 
case when tags.text127.attr_value<>'' then tags.text127.attr_value else default_article['COM'] end attr68, 
case when tags.text138.attr_value<>'' then tags.text138.attr_value else default_article['COM'] end attr69, 
case when tags.text135.attr_value<>'' then tags.text135.attr_value else default_article['COM'] end attr70, 
case when tags.text137.attr_value<>'' then tags.text137.attr_value else default_article['COM'] end attr71,
case when tags.text198.attr_value<>'' then tags.text198.attr_value else default_article['WMS'] end attr73,
case when tags.text193.attr_value<>'' then tags.text193.attr_value else default_article['WMS'] end attr74,
case when tags.text199.attr_value<>'' then tags.text199.attr_value else default_article['WMS'] end attr75,
case when tags.text196.attr_value<>'' then tags.text196.attr_value else default_article['COM'] end attr76,
case when tags.text197.attr_value<>'' then tags.text197.attr_value else default_article['COM'] end attr77,
case when tags.text195.attr_value<>'' then tags.text195.attr_value else default_article['COM'] end attr78,
case when tags.text192.attr_value<>'' then tags.text192.attr_value else default_article['APP'] end attr79,
case when tags.text194.attr_value<>'' then tags.text194.attr_value else default_article['APP'] end attr80,
case when tags.text200.attr_value<>'' then tags.text200.attr_value else default_article['APP'] end attr81,
case when tags.text202.attr_value<>'' then tags.text202.attr_value else '' end attr82,
case when tags.text203.attr_value<>'' then tags.text203.attr_value else '' end attr83,
case when tags.text204.attr_value<>'' then tags.text204.attr_value else default_article['WMS'] end attr21, 
case when tags.text206.attr_value<>'' then tags.text206.attr_value else default_article['WMS'] end attr27, 
case when tags.text205.attr_value<>'' then tags.text205.attr_value else default_article['WMS'] end attr32, 
case when tags.text210.attr_value<>'' then tags.text210.attr_value else default_article['APP'] end attr23, 
case when tags.text208.attr_value<>'' then tags.text208.attr_value else default_article['APP'] end attr26, 
case when tags.text211.attr_value<>'' then tags.text211.attr_value else default_article['APP'] end attr30, 
case when tags.text212.attr_value<>'' then tags.text212.attr_value else default_article['COM'] end attr31, 
case when tags.text207.attr_value<>'' then tags.text207.attr_value else default_article['COM'] end attr33, 
case when tags.text209.attr_value<>'' then tags.text209.attr_value else default_article['COM'] end attr38 
from etl_update_customer_value_tag v 
cross join (select map_from_entries(collect_set(struct(platform,article_code))) default_article from ods_f1103_pre_hot_sales where create_day={date_key} and seq_no_cdp=1) h
where v.create_day={date_key}) v
left outer join customer_attribute ct on ct.tenant_id=1 and v.customer_id=ct.customer_id,
null,
null
),



