

SELECT t2.userid,
       t2.visitmonth,
       subtotal_visit_cnt,
       sum(subtotal_visit_cnt) over (partition BY userid ORDER BY visitmonth) AS total_visit_cnt
FROM
  (SELECT userid,
          visitmonth,
          sum(visitcount) AS subtotal_visit_cnt
   FROM
     (SELECT userid,
             date_format(regexp_replace(visitdate,'-','/'),'yyyy-MM') AS visitmonth,
             visitcount
      FROM test_sql.test1) t1
   GROUP BY userid,
            visitmonth)t2
ORDER BY t2.userid,
         t2.visitmonth
		 	 

SELECT t2.shop,
       t2.user_id,
       t2.cnt
FROM
  (SELECT t1.*,
          row_number() over(partition BY t1.shop
                            ORDER BY t1.cnt DESC) rank
   FROM
     (SELECT user_id,
             shop,
             count(*) AS cnt
      FROM test_sql.test2
      GROUP BY user_id,
               shop) t1)t2
WHERE rank <= 3; 


有一个5000万的用户文件(user_id，name，age)，一个2亿记录的用户看电影的记录文件(user_id，url)，根据年龄段观看电影的次数进行排序？        


SELECT sum(total_user_cnt) total_user_cnt,
       sum(total_user_avg_age) total_user_avg_age,
       sum(two_days_cnt) two_days_cnt,
       sum(avg_age) avg_age
FROM
  (SELECT 0 total_user_cnt,
          0 total_user_avg_age,
          count(*) AS two_days_cnt,
          cast(sum(age) / count(*) AS decimal(5,2)) AS avg_age
   FROM
     (SELECT user_id,
             max(age) age
      FROM
        (SELECT user_id,
                max(age) age
         FROM
           (SELECT user_id,
                   age,
                   date_sub(dt,rank) flag
            FROM
              (SELECT dt,
                      user_id,
                      max(age) age,
                      row_number() over(PARTITION BY user_id
                                        ORDER BY dt) rank
               FROM test_sql.test5
               GROUP BY dt,
                        user_id) t1) t2
         GROUP BY user_id,
                  flag
         HAVING count(*) >=2) t3
      GROUP BY user_id) t4
   UNION ALL SELECT count(*) total_user_cnt,
                    cast(sum(age) /count(*) AS decimal(5,2)) total_user_avg_age,
                    0 two_days_cnt,
                    0 avg_age
   FROM
     (SELECT user_id,
             max(age) age
      FROM test_sql.test5
      GROUP BY user_id) t5) t6


CREATE TABLE test1 ( 
        userId string, 
        visitDate string,
        visitCount INT )
    ROW format delimited FIELDS TERMINATED BY "\t";
    INSERT INTO TABLE test1
    VALUES
        ( 'u01', '2017/1/21', 5 ),
        ( 'u02', '2017/1/23', 6 ),
        ( 'u03', '2017/1/22', 8 ),
        ( 'u04', '2017/1/20', 3 ),
        ( 'u01', '2017/1/23', 6 ),
        ( 'u01', '2017/2/21', 8 ),
        ( 'u02', '2017/1/23', 6 ),
        ( 'u01', '2017/2/22', 4 );



SELECT t2.userid,
       t2.visitmonth,
       subtotal_visit_cnt,
       sum(subtotal_visit_cnt) over (partition BY userid ORDER BY visitmonth) AS total_visit_cnt
FROM
  (SELECT userid,
          visitmonth,
          sum(visitcount) AS subtotal_visit_cnt
   FROM
     (SELECT userid,
             date_format(regexp_replace(visitdate,'/','-'),'yyyy-MM') AS visitmonth,
             visitcount
      FROM user) t1
   GROUP BY userid,
            visitmonth)t2
ORDER BY t2.userid,
         t2.visitmonth
		 
		 

		 CREATE TABLE test_sql.book(book_id string,
                           `SORT` string,
                           book_name string,
                           writer string,
                           OUTPUT string,
                           price decimal(10,2));
INSERT INTO TABLE test_sql.book VALUES ('001','TP391','信息处理','author1','机械工业出版社','20','qqqq','dddddd');
INSERT INTO TABLE test_sql.book VALUES ('002','TP392','数据库','author12','科学出版社','15');
INSERT INTO TABLE test_sql.book VALUES ('003','TP393','计算机网络','author3','机械工业出版社','29');
INSERT INTO TABLE test_sql.book VALUES ('004','TP399','微机原理','author4','科学出版社','39');
INSERT INTO TABLE test_sql.book VALUES ('005','C931','管理信息系统','author5','机械工业出版社','40');
INSERT INTO TABLE test_sql.book VALUES ('006','C932','运筹学','author6','科学出版社','55');


-- 创建读者表reader

CREATE TABLE test_sql.reader (reader_id string,
                              company string,
                              name string,
                              sex string, 
                              grade string,
                              addr string);
INSERT INTO TABLE test_sql.reader VALUES ('0001','阿里巴巴','jack','男','vp','addr1');
INSERT INTO TABLE test_sql.reader VALUES ('0002','百度','robin','男','vp','addr2');
INSERT INTO TABLE test_sql.reader VALUES ('0003','腾讯','tony','男','vp','addr3');
INSERT INTO TABLE test_sql.reader VALUES ('0004','京东','jasper','男','cfo','addr4');
INSERT INTO TABLE test_sql.reader VALUES ('0005','网易','zhangsan','女','ceo','addr5');
INSERT INTO TABLE test_sql.reader VALUES ('0006','搜狐','lisi','女','ceo','addr6');

-- 创建借阅记录表borrow_log

CREATE TABLE test_sql.borrow_log(reader_id string,
                                 book_id string,
                                 borrow_date string);

INSERT INTO TABLE test_sql.borrow_log VALUES ('0001','002','2019-10-14');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0002','001','2019-10-13');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0003','005','2019-09-14');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0004','006','2019-08-15');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0005','003','2019-10-10');
INSERT INTO TABLE test_sql.borrow_log VALUES ('0006','004','2019-17-13');

select * from reader r left semi join reader b on r.reader_id=b.reader_id;
		 
select v.*, 
        case when new_clientid like '0rand%' then null else substr(new_clientid,14) end new_clientid1,
        row_number() over(partition by case when new_clientid like '0rand%' then new_clientid else substr(new_clientid,14) 
     end order by case when memberid<>'' then 1 else 0 end desc,firstlaunchdate desc) rn1 
    from(select v.*,
            case when v.memberid<>'' then v.memberid else concat('rand',v.deviceid) end new_memberid,
            max(case when v.clientid<>'' then concat(v.firstlaunchdate,v.clientid) else concat('0rand',v.deviceid) end) over(partition by deviceid) new_clientid,
            min(v.createtime) over(partition by deviceid) min_createtime,
            row_number() over(partition by deviceid order by case when memberid<>'' then 1 else 0 end desc,firstlaunchdate desc) rn 
        from etl_pull_app_visitor_record v where v.create_day={date_key} and v.deviceid<>'' and firstlaunchdate<>'' and v.os in('iOS','Android')) v where rn=1) v




select * from student s where class_name exits ('1班','2班') t1

select 
    t3.tn '老师名',
	avg(t3.sc) '平均分',
from
  (select 
        t1.class_name cn,
        t1.scoure sc,
        t2.t_name tn 
   from (select * from student s where class_name exits ('1班','2班')) t1 
       left outer join classe t2 
           on t1.class_name=t2.class_name) t3 group by t3.cn
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
+------+----------+------------------+
|userid|visitmonth|subtotal_visit_cnt|
+------+----------+------------------+
|   u04|   2017-01|                 3|
|   u01|   2017-01|                11|
|   u03|   2017-01|                 8|
|   u01|   2017-02|                12|
|   u02|   2017-01|                12|
+------+----------+------------------+


|userid|visitmonth|subtotal_visit_cnt|total_visit_cnt|
+------+----------+------------------+---------------+
|   u01|   2017-01|                11|             11|
|   u01|   2017-02|                12|             23|
|   u02|   2017-01|                12|             12|
|   u03|   2017-01|                 8|              8|
|   u04|   2017-01|                 3|              3|
+------+----------+------------------+---------------+

|userid|visitmonth|subtotal_visit_cnt|total_visit_cnt|
+------+----------+------------------+---------------+
|   u01|   2017-01|                11|             11|
|   u01|   2017-02|                12|             23|
|   u02|   2017-01|                12|             12|
|   u04|   2017-01|                 3|              3|
|   u03|   2017-01|                 8|              8|
+------+----------+------------------+---------------+

select u1.uname,
       u1.datas,
   u1.ucs,
   sum(ucs)over(partition by u1.uname) order by u1.uname)) rn 
from(select uname,
            date_format(regexp_replace(udate,'/','-'),'yyyy-MM') as datas,
            ucs
       from user) u1
       group by u1.uname,
                u1.datas;
				
				
				
				'select count(1)=1 from etl_task_run where task_name in(''etl_pull_app_visitor'') and period=''{date_id}'' and status=''Succeed''',
'select 
case when update_flag=3 then identity_customer_id
    else customer_id end customer_id,
deviceid,
clientid from etl_pull_app_visitor where create_day={date_key} and update_flag in(1,2,3)',