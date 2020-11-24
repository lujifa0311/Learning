--hive分析函数
案例

数据准备
CREATE TABLE `orders` (
    `order_num` String COMMENT '订单号',
    `order_amount` DECIMAL ( 12, 2 ) COMMENT '订单金额',
    `advance_amount` DECIMAL ( 12, 2 ) COMMENT '预付款',
    `order_date` string COMMENT '订单日期',
    `cust_code` string COMMENT '客户',
    `agent_code` string COMMENT '代理商' 
);
INSERT INTO orders VALUES('200100', '1000.00', '600.00', '2020-08-01', 'C00013', 'A003');
INSERT INTO orders VALUES('200110', '3000.00', '500.00', '2020-04-15', 'C00019', 'A010');
INSERT INTO orders VALUES('200107', '4500.00', '900.00', '2020-08-30', 'C00007', 'A010');
INSERT INTO orders VALUES('200112', '2000.00', '400.00', '2020-05-30', 'C00016', 'A007'); 
INSERT INTO orders VALUES('200113', '4000.00', '600.00', '2020-06-10', 'C00022', 'A002');
INSERT INTO orders VALUES('200102', '2000.00', '300.00', '2020-05-25', 'C00012', 'A012');
INSERT INTO orders VALUES('200114', '3500.00', '2000.00', '2020-08-15', 'C00002','A008');
INSERT INTO orders VALUES('200122', '2500.00', '400.00', '2020-09-16', 'C00003', 'A004');
INSERT INTO orders VALUES('200118', '500.00', '100.00', '2020-07-20', 'C00023', 'A006');
INSERT INTO orders VALUES('200119', '4000.00', '700.00', '2020-09-16', 'C00007', 'A010');
INSERT INTO orders VALUES('200121', '1500.00', '600.00', '2020-09-23', 'C00008', 'A004');
INSERT INTO orders VALUES('200130', '2500.00', '400.00', '2020-07-30', 'C00025', 'A011');
INSERT INTO orders VALUES('200134', '4200.00', '1800.00', '2020-09-25', 'C00004','A005');
INSERT INTO orders VALUES('200108', '4000.00', '600.00', '2020-02-15', 'C00008', 'A004');
INSERT INTO orders VALUES('200103', '1500.00', '700.00', '2020-05-15', 'C00021', 'A005');
INSERT INTO orders VALUES('200105', '2500.00', '500.00', '2020-07-18', 'C00025', 'A011');
INSERT INTO orders VALUES('200109', '3500.00', '800.00', '2020-07-30', 'C00011', 'A010');
INSERT INTO orders VALUES('200101', '3000.00', '1000.00', '2020-07-15', 'C00001','A008');
INSERT INTO orders VALUES('200111', '1000.00', '300.00', '2020-07-10', 'C00020', 'A008');
INSERT INTO orders VALUES('200104', '1500.00', '500.00', '2020-03-13', 'C00006', 'A004');
INSERT INTO orders VALUES('200106', '2500.00', '700.00', '2020-04-20', 'C00005', 'A002');
INSERT INTO orders VALUES('200125', '2000.00', '600.00', '2020-10-01', 'C00018', 'A005');
INSERT INTO orders VALUES('200117', '800.00', '200.00', '2020-10-20', 'C00014', 'A001');
INSERT INTO orders VALUES('200123', '500.00', '100.00', '2020-09-16', 'C00022', 'A002');
INSERT INTO orders VALUES('200120', '500.00', '100.00', '2020-07-20', 'C00009', 'A002');
INSERT INTO orders VALUES('200116', '500.00', '100.00', '2020-07-13', 'C00010', 'A009');
INSERT INTO orders VALUES('200124', '500.00', '100.00', '2020-06-20', 'C00017', 'A007'); 
INSERT INTO orders VALUES('200126', '500.00', '100.00', '2020-06-24', 'C00022', 'A002');
INSERT INTO orders VALUES('200129', '2500.00', '500.00', '2020-07-20', 'C00024', 'A006');
INSERT INTO orders VALUES('200127', '2500.00', '400.00', '2020-07-20', 'C00015', 'A003');
INSERT INTO orders VALUES('200128', '3500.00', '1500.00', '2020-07-20', 'C00009','A002');
INSERT INTO orders VALUES('200135', '2000.00', '800.00', '2020-09-16', 'C00007', 'A010');
INSERT INTO orders VALUES('200131', '900.00', '150.00', '2020-08-26', 'C00012', 'A012');
INSERT INTO orders VALUES('200133', '1200.00', '400.00', '2020-06-29', 'C00009', 'A002');


AVG() 和SUM()
需求描述：
第三季度每个代理商的移动平均收入和总收入
SELECT
    agent_code,
    order_date,
    AVG( order_amount ) OVER ( PARTITION BY agent_code ORDER BY order_date)  avg_rev,
    SUM( order_amount ) OVER ( PARTITION BY agent_code ORDER BY order_date ) total_rev 
FROM
orders 
WHERE
order_date >= '2020-07-01' 
AND order_date <= '2020-09-30';

结果输出
A002    2020-07-20      2000    4000
A002    2020-07-20      2000    4000
A002    2020-09-16      1500    4500
A003    2020-07-20      2500    2500
A003    2020-08-01      1750    3500
A004    2020-09-16      2500    2500
A004    2020-09-23      2000    4000
A005    2020-09-25      4200    4200
A006    2020-07-20      1500    3000
A006    2020-07-20      1500    3000
A008    2020-07-10      1000    1000
A008    2020-07-15      2000    4000
A008    2020-08-15      2500    7500
A009    2020-07-13      500     500
A010    2020-07-30      3500    3500
A010    2020-08-30      4000    8000
A010    2020-09-16      3500    14000
A010    2020-09-16      3500    14000
A011    2020-07-18      2500    2500
A011    2020-07-30      2500    5000
A012    2020-08-26      900     900


FIRST_VALUE()和 LAST_VALUE()
first_value: 取分组内排序后，截止到当前行，第一个值
last_value: 取分组内排序后，截止到当前行，最后一个值

需求描述
客户首次购买后多少天才进行下一次购买
SELECT
    cust_code,
    order_date,
    datediff(order_date,FIRST_VALUE ( order_date ) OVER ( PARTITION BY cust_code ORDER BY order_date )) next_order_gap 
FROM
orders 
order by cust_code,next_order_gap


结果输出
C00001  2020-07-15      0
C00002  2020-08-15      0
C00003  2020-09-16      0
C00004  2020-09-25      0
C00005  2020-04-20      0
C00006  2020-03-13      0
C00007  2020-08-30      0
C00007  2020-09-16      17
C00007  2020-09-16      17
C00008  2020-02-15      0
C00008  2020-09-23      221
C00009  2020-06-29      0
C00009  2020-07-20      21
C00009  2020-07-20      21
C00010  2020-07-13      0
C00011  2020-07-30      0
C00012  2020-05-25      0
C00012  2020-08-26      93
C00013  2020-08-01      0
C00014  2020-10-20      0
C00015  2020-07-20      0
C00016  2020-05-30      0
C00017  2020-06-20      0
C00018  2020-10-01      0
C00019  2020-04-15      0
C00020  2020-07-10      0
C00021  2020-05-15      0
C00022  2020-06-10      0
C00022  2020-06-24      14
C00022  2020-09-16      98
C00023  2020-07-20      0
C00024  2020-07-20      0
C00025  2020-07-18      0
C00025  2020-07-30      12


LEAD() 和 LAG()
lead(value_expr[,offset[,default]])：用于统计窗口内往下第n行值。第一个参数为列名，第二个参数为往下第n行（可选，默认为1），第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL
lag(value_expr[,offset[,default]]): 与lead相反，用于统计窗口内往上第n行值。第一个参数为列名，第二个参数为往上第n行（可选，默认为1），第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）
需求描述
代理商最近一次出售的最高订单金额是多少？

SELECT
 agent_code,
 order_amount,
 LAG ( order_amount, 1 ) OVER ( PARTITION BY agent_code ORDER BY order_amount DESC ) last_highest_amount 
FROM
 orders 
ORDER BY
 agent_code,
 order_amount DESC;


结果输出
A001    800     NULL
A002    4000    NULL
A002    3500    4000
A002    2500    3500
A002    1200    2500
A002    500     1200
A002    500     500
A002    500     500
A003    2500    NULL
A003    1000    2500
A004    4000    NULL
A004    2500    4000
A004    1500    2500
A004    1500    1500
A005    4200    NULL
A005    2000    4200
A005    1500    2000
A006    2500    NULL
A006    500     2500
A007    2000    NULL
A007    500     2000
A008    3500    NULL
A008    3000    3500
A008    1000    3000
A009    500     NULL
A010    4500    NULL
A010    4000    4500
A010    3500    4000
A010    3000    3500
A010    2000    3000
A011    2500    NULL
A011    2500    2500
A012    2000    NULL
A012    900     2000


RANK() 和DENSE_RANK()
rank：对组中的数据进行排名，如果名次相同，则排名也相同，但是下一个名次的排名序号会出现不连续。比如查找具体条件的topN行。RANK() 排序为 (1,2,2,4)
dense_rank：dense_rank函数的功能与rank函数类似，dense_rank函数在生成序号时是连续的，而rank函数生成的序号有可能不连续。当出现名次相同时，则排名序号也相同。而下一个排名的序号与上一个排名序号是连续的。
DENSE_RANK() 排序为 (1,2,2,3)
需求描述
每月第二高的订单金额是多少？
SELECT
 order_num,
 order_date,
 order_amount,
 order_month 
FROM
 (
SELECT
 order_num,
 order_date,
 order_amount,
 DATE_FORMAT( order_date, 'YYYY-MM' ) AS order_month,
 DENSE_RANK ( ) OVER ( PARTITION BY DATE_FORMAT( order_date, 'YYYY-MM' ) ORDER BY order_amount DESC ) order_rank 
FROM
 orders 
 ) t 
WHERE
 order_rank = 2 
ORDER BY
 order_date;
结果输出
200106  2020-04-20      2500    2020-04
200103  2020-05-15      1500    2020-05
200133  2020-06-29      1200    2020-06
200101  2020-07-15      3000    2020-07
200114  2020-08-15      3500    2020-08
200119  2020-09-16      4000    2020-09
200117  2020-10-20      800     2020-10


CUME_DIST()
cume_dist:如果按升序排列，则统计：小于等于当前值的行数/总行数(number of rows ≤ current row)/(total number of rows）。如果是降序排列，则统计：大于等于当前值的行数/总行数。比如，统计小于等于当前工资的人数占总人数的比例 ，用于累计统计。
需求描述
8月和9月每个订单的收入百分比
先查看一下8月和9月的数据，按订单金额排序

SELECT
 order_num,
 order_amount,
 order_date,
 agent_code 
FROM
 orders 
WHERE
 order_date >= '2020-08-01' 
 AND order_date <= '2020-09-30' 
ORDER BY
 date_format( order_date, "YYYY-MM" ),
 order_amount;
其结果为：

SELECT
 DATE_FORMAT( order_date, 'YYYY-MM' ) AS order_month,
 agent_code,
 order_amount,
 CUME_DIST ( ) OVER ( PARTITION BY DATE_FORMAT( order_date, 'YYYY-MM' ) ORDER BY order_amount ) 
FROM
 orders 
WHERE
 order_date >= '2020-08-01' 
 AND order_date <= '2020-09-30';
结果输出
2020-08 A012    900     0.25
2020-08 A003    1000    0.5
2020-08 A008    3500    0.75
2020-08 A010    4500    1.0
2020-09 A002    500     0.16666666666666666
2020-09 A004    1500    0.3333333333333333
2020-09 A010    2000    0.5
2020-09 A004    2500    0.6666666666666666
2020-09 A010    4000    0.8333333333333334
2020-09 A005    4200    1.0