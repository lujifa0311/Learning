--导入群组

(100000000000000000+{date_key}*10000000L+1 list_id
concat('prom','-','-{date_key}') list_name

--插入任务定义
select 







------修改属性------

--建表
CREATE TABLE ods_promo_member(
member_id string,
apromo1 string,
apromo2 string,
apromo3 string,
apromo4 string,
apromo5 string)
PARTITIONED by(o_date int)
STORED AS textfile;

CREATE TABLE ods_promo_p1(
b1promo1 string,
b1promo2 string,
b1promo3 string,
b1promo4 string,
b1promo5 string)
PARTITIONED by(o_date int)
STORED AS textfile;

CREATE TABLE ods_promo_p2(
b2promo2 string)
PARTITIONED by(o_date int)
STORED AS textfile;

CREATE TABLE ods_promo_p3(
b3promo3 string)
PARTITIONED by(o_date int)
STORED AS textfile;

CREATE TABLE ods_promo_p4(
b4promo4 string)
PARTITIONED by(o_date int)
STORED AS textfile;

CREATE TABLE ods_promo_p5(
b5promo5 string)
PARTITIONED by(o_date int)
STORED AS textfile;

CREATE TABLE etl_update_promo_customer_attribute(
member_id,
promo1,
promo2,
promo3,
promo4,
promo5,
customer_id,
customer_attribute_id)
PARTITIONED by(o_date int)
STORED AS parquet;


--promo关联根据memberid关联etl_pull_customer再关联customer_attribute查询属性值
insert overwrite table etl_update_promo_customer_attribute partition(20200824)
select 
   a.member_id,
   nvl(b.bpromo1,nvl(b1.b1promo1,c1.attr39)),
   nvl(b.bpromo2,nvl(b1.b1promo2,nvl(a1.b2promo2,ci.attr42))),
   nvl(b.bpromo3,nvl(b1.b1promo3,nvl(a1.b3promo3,ci.attr48))),
   nvl(b.bpromo4,nvl(b1.b1promo4,nvl(a1.b4promo4,ci.attr49))),
   nvl(b.bpromo5,nvl(b1.b1promo5,nvl(a1.b5promo5,ci.attr50))),
   c.id,
   ci.id
from (select * 
      from (select a.*,row_number() over(order by memberid) rn from ods_promo_member a where o_date=20200824)) a 
	left join (select b1.*,row_number() over(order by b1promo1) rn from ods_promo1 b1 where o_date=20200824) b1 on b1.rn=a.rn 
    left join (select b2.*,row_number() over(order by b1promo2) rn from ods_promo2 b2 where o_date=20200824) b2 on b2.rn=a.rn
    left join (select b3.*,row_number() over(order by b1promo3) rn from ods_promo3 b3 where o_date=20200824) b3 on b3.rn=a.rn
    left join (select b4.*,row_number() over(order by b1promo4) rn from ods_promo4 b4 where o_date=20200824) b4 on b4.rn=a.rn
    left join (select b5.*,row_number() over(order by b1promo5) rn from ods_promo5 b5 where o_date=20200824) b5 on b5.rn=a.rn
left out join etl_pull_customer c on c.create_day=20200824 and c.update_flag>=0 and a1.memberid = c.member_id 
left out join customer_attribute ci on c.member_id = ci.attr1;

--修改属性值
update xiaoshu.customer_attribute set attr39=?,attr42=?,attr48=?,attr49=?,attr50=? where id=?


--promo导入群组

insert into list(id,name,tenant_id,version,date_created,last_updated,auto_update,is_static,is_system,deleted,total)values
(200000000000000001+20200824*10000000L,'Newlook_20200826',1,0,now(),now(),true,true,false,false,0)


select customer_id from etl_update_promo_customer_attribute where o_date=20200824

insert ignore into xiaoshu.list_member(version,customer_id,list_id,tenant_id,date_created,last_updated,_etl_source) values (0,?,200000000000000001+20200824*10000000L,1,now(),now(),null)



