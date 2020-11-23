1、分别给出四张表的数据分别是：
student_info.txt
字段是：学号,姓名,性别,所属班级编号,入学日期
department_info.txt
字段是：院系编号,院系名称
class_info.txt
字段是：班级编号,班级名称,入学日期,所属院系中文名
student_scores.txt
字段是：学号,姓名,性别,所属班级编号,入学成绩
请分别使用三种方式
第1种：指定列名添加Schema
第2种：通过StructType指定Schema
第3种：编写样例类，利用反射机制推断Schema
查询四个文件的数据

object HomeWork20200413_1 {

  case class student_info(userID:String,userName:String,sex:String,classID:String,date:String)
  case class department_info(departmentID:String,departmentName:String)
  case class class_info(classID:String,className:String,date:String,departmentName:String)
  case class student_scores(userID:String,username:String,sex:String,classID:String,score:String)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("HomeWork20200413_1")
      .config(new SparkConf())
      .getOrCreate()
    import spark.implicits._

    /**
     * 第一题
     */

    /**
     * 方式1：指定列名添加Schema
     */
    val rdd1: RDD[String] = spark.sparkContext.textFile("input20200413/student_info.txt")
    val rdd2: RDD[String] = spark.sparkContext.textFile("input20200413/department_info.txt")
    val rdd3: RDD[String] = spark.sparkContext.textFile("input20200413/class_info.txt")
    val rdd4: RDD[String] = spark.sparkContext.textFile("input20200413/student_scores.txt")
    rdd1.map{x => var datas = x.split(",");(datas(0),datas(1),datas(2),datas(3),datas(4))}.toDF("userID","userName","sex","classID","date").show()
    rdd2.map{x => var datas = x.split(",");(datas(0),datas(1))}.toDF("departmentID","departmentName").show()
    rdd3.map{x => var datas = x.split(",");(datas(0),datas(1),datas(2),datas(3))}.toDF("classID","className","date","departmentName").show()
    rdd4.map{x => var datas = x.split(",");(datas(0),datas(1),datas(2),datas(3),datas(4))}.toDF("userID","username","sex","classID","score").show()

    /**
     * 方式2：通过StructType指定Schema
     */
    val rdd11: RDD[String] = spark.sparkContext.textFile("input20200413/student_info.txt")
    val rdd22: RDD[String] = spark.sparkContext.textFile("input20200413/department_info.txt")
    val rdd33: RDD[String] = spark.sparkContext.textFile("input20200413/class_info.txt")
    val rdd44: RDD[String] = spark.sparkContext.textFile("input20200413/student_scores.txt")
    val rowRDD1: RDD[Row] = rdd11.map(_.split(",")).map(x => Row(x(0),x(1),x(2),x(3),x(4)))
    val rowRDD2: RDD[Row] = rdd22.map(_.split(",")).map(x => Row(x(0),x(1)))
    val rowRDD3: RDD[Row] = rdd33.map(_.split(",")).map(x => Row(x(0),x(1),x(2),x(3)))
    val rowRDD4: RDD[Row] = rdd44.map(_.split(",")).map(x => Row(x(0),x(1),x(2),x(3),x(4)))
    val structType1:StructType = StructType(Seq(
      StructField("userID", StringType),
      StructField("userName", StringType),
      StructField("sex", StringType),
      StructField("classID", StringType),
      StructField("date", StringType)
    ))
    val structType2:StructType = StructType(Array(
      StructField("departmentID",StringType),
      StructField("departmentName",StringType)
    ))
    val structType3:StructType = StructType(Array(
      StructField("classID",StringType),
      StructField("className",StringType),
      StructField("date",StringType),
      StructField("departmentName",StringType)
    ))
    val structType4:StructType = StructType(Array(
      StructField("userID",StringType),
      StructField("username",StringType),
      StructField("sex",StringType),
      StructField("classID",StringType),
      StructField("score",StringType)
    ))
    spark.createDataFrame(rowRDD1,structType1).show()
    spark.createDataFrame(rowRDD2,structType2).show()
    spark.createDataFrame(rowRDD3,structType3).show()
    spark.createDataFrame(rowRDD4,structType4).show()

    /**
     * 方式3：编写样例类，利用反射机制推断Schema
     */
    val rdd111: RDD[String] = spark.sparkContext.textFile("input20200413/student_info.txt")
    val rdd222: RDD[String] = spark.sparkContext.textFile("input20200413/department_info.txt")
    val rdd333: RDD[String] = spark.sparkContext.textFile("input20200413/class_info.txt")
    val rdd444: RDD[String] = spark.sparkContext.textFile("input20200413/student_scores.txt")
    val caseRDD1: RDD[student_info] = rdd111.map(_.split(",")).map(x => student_info(x(0),x(1),x(2),x(3),x(4)))
    val caseRDD2: RDD[department_info] = rdd222.map(_.split(",")).map(x => department_info(x(0),x(1)))
    val caseRDD3: RDD[class_info] = rdd333.map(_.split(",")).map(x => class_info(x(0),x(1),x(2),x(3)))
    val caseRDD4: RDD[student_scores] = rdd444.map(_.split(",")).map(x => student_scores(x(0),x(1),x(2),x(3),x(4)))
    caseRDD1.toDF().show()
    caseRDD2.toDF().show()
    caseRDD3.toDF().show()
    caseRDD4.toDF().show()

    spark.stop()
  }
}





2、在hive中创建表
用户行为表：user_visit_action  对应的数据文件是user_visit_action.txt,分割符为tab键
字段依次是：时间 用户id 会话id 页面id 时间戳 搜索关键字 点击品类id 点击产品id 下单品类id 下单产品id 支付品类ids 支付产品ids 城市id
城市表：city_info 对应的数据文件是city_info.txt,分割符为tab键
字段依次是：城市id 城市名字 地区
产品表：product_info 对应的数据文件是product_info.txt,分割符为tab键
字段依次是：产品id 产品名字 店铺类型
CREATE TABLE `user_visit_action`(
  `date` string,
  `user_id` bigint,
  `session_id` string,
  `page_id` bigint,
  `action_time` string,
  `search_keyword` string,
  `click_category_id` bigint,
  `click_product_id` bigint,
  `order_category_ids` string,
  `order_product_ids` string,
  `pay_category_ids` string,
  `pay_product_ids` string,
  `city_id` bigint)
row format delimited fields terminated by '\t';
CREATE TABLE `product_info`(
  `product_id` bigint,
  `product_name` string,
  `extend_info` string)
row format delimited fields terminated by '\t';
CREATE TABLE `city_info`(
  `city_id` bigint,
  `city_name` string,
  `area` string)
row format delimited fields terminated by '\t';
3、分别导入数据到hive中的3张表中
4、请使用spark sql 计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。

object HomeWork20200413_2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("HomeWork20200413_2")
      .config(new SparkConf())
      .enableHiveSupport()
      .getOrCreate()

    /**
     * 第2-4题
     */
    spark.udf.register("remark",new Remark())
    spark.sql(
      """
        |select area,product_id,product_count,remark from (
        |select remark(city_name) remark,ci.area,pi.product_id,count(pi.product_id) product_count,row_number() over (partition by ci.area order by count(pi.product_id) desc) rk
        |from homework_20200413.city_info ci
        |         left join homework_20200413.user_visit_action uva
        |                   on ci.city_id = uva.city_id
        |         left join homework_20200413.product_info pi
        |                   on uva.click_product_id = pi.product_id
        |group by ci.area,pi.product_id
        |order by ci.area,product_count desc) tmpA
        |where tmpA.rk <= 3
        |""".stripMargin).show()
    spark.stop()
  }




  class Remark extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(StructField("city_name",StringType)::Nil)

    override def bufferSchema: StructType = StructType(StructField("buffer",MapType(StringType,LongType))::StructField("count",LongType)::Nil)

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Map[String,Long]()
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if(!input.isNullAt(0)){
        val city_name: String = input.getString(0)
        val temp_map: collection.Map[String, Long] =  buffer.getMap[String,Long](0)
        buffer(0) = temp_map + (city_name->(temp_map.getOrElse(city_name,0L)+1L))
        buffer(1) = buffer.getLong(1) + 1L
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val temp_map1: collection.Map[String, Long] =  buffer1.getMap[String,Long](0)
      val temp_map2: collection.Map[String, Long] =  buffer2.getMap[String,Long](0)
      val count1: Long = buffer1.getLong(1)
      val count2: Long = buffer2.getLong(1)
      buffer1(0) = temp_map1.foldLeft(temp_map2){
        case (map,(k,v))=>map + ( k ->(map.getOrElse(k,1L)+v))
      }
      buffer1(1) = count1+count2

    }

    override def evaluate(buffer: Row): Any = {
      val count: Long = buffer.getLong(1)
      val format = new DecimalFormat(".00%")
      val tuples: List[(String, Long)] = buffer.getMap[String, Long](0).toList.sortBy(-_._2).take(2)
      println(tuples)
      val countTop2: Long = tuples.foldLeft(0L)((x, y) => x + y._2)
      tuples.map {
        case (k, v) => {
          k + ":" + format.format(v.toDouble/count)
        }
      }.mkString(", ") + ", 其他" +":"+ format.format((count-countTop2).toDouble / count)
    }
  }

}


5、
某网站包含两个表，Customers 表和 Orders 表。编写一个 SQL 查询，找出所有从不订购任何东西的客户。

Customers 表：

+----+-------+
| Id | Name  |
+----+-------+
| 1  | Joe   |
| 2  | Henry |
| 3  | Sam   |
| 4  | Max   |
+----+-------+
Orders 表：

+----+------------+
| Id | CustomerId |
+----+------------+
| 1  | 3          |
| 2  | 1          |
+----+------------+
例如给定上述表格，你的查询应返回：

+-----------+
| Customers |
+-----------+
| Henry     |
| Max       |
+-----------+

spark.sql(
      """
        |SELECT NAME FROM
        |(
        |SELECT o.id,NAME FROM homework_20200413.customers c LEFT JOIN homework_20200413.orders o
        |ON c.id = o.id AND o.id IS NOT NULL
        |) tmp
        |WHERE id IS NULL
        |""".stripMargin).show()



6、
Employee 表包含所有员工信息，每个员工有其对应的 Id, salary 和 department Id。
+----+-------+--------+--------------+
| Id | Name  | Salary | DepartmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 70000  | 1            |
| 2  | Henry | 80000  | 2            |
| 3  | Sam   | 60000  | 2            |
| 4  | Max   | 90000  | 1            |
+----+-------+--------+--------------+
Department 表包含公司所有部门的信息。

+----+----------+
| Id | Name     |
+----+----------+
| 1  | IT       |
| 2  | Sales    |
+----+----------+
编写一个 SQL 查询，找出每个部门工资最高的员工。例如，根据上述给定的表格，Max 在 IT 部门有最高工资，Henry 在 Sales 部门有最高工资。

+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Max      | 90000  |
| Sales      | Henry    | 80000  |
+------------+----------+--------+

spark.sql(
  """
    |select dname, ename, salary from
    |(
    |SELECT d.name dname,e.name ename,salary,row_number() over(PARTITION BY d.id ORDER BY salary desc) rk
    |FROM homework_20200413.employee e JOIN homework_20200413.department d ON e.departmentid = d.id
    |) tmp
    |where rk = 1
    |""".stripMargin).show()



 7、
 Employee 表包含所有员工信息，每个员工有其对应的 Id, salary 和 department Id 。
+----+-------+--------+--------------+
| Id | Name  | Salary | DepartmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 70000  | 1            |
| 2  | Henry | 80000  | 2            |
| 3  | Sam   | 60000  | 2            |
| 4  | Max   | 90000  | 1            |
| 5  | Janet | 69000  | 1            |
| 6  | Randy | 85000  | 1            |
+----+-------+--------+--------------+
Department 表包含公司所有部门的信息。

+----+----------+
| Id | Name     |
+----+----------+
| 1  | IT       |
| 2  | Sales    |
+----+----------+
编写一个 SQL 查询，找出每个部门工资前三高的员工。例如，根据上述给定的表格，查询结果应返回：
+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Max      | 90000  |
| IT         | Randy    | 85000  |
| IT         | Joe      | 70000  |
| Sales      | Henry    | 80000  |
| Sales      | Sam      | 60000  |
+------------+----------+--------+

    spark.sql(
      """
        |select dname, ename, salary from
        |(
        |SELECT d.name dname,e.name ename,salary,row_number() over(PARTITION BY d.id ORDER BY salary desc) rk
        |FROM homework_20200413.employee e JOIN homework_20200413.department d ON e.departmentid = d.id
        |) tmp
        |where rk <= 3
        |""".stripMargin).show()

8、
编写一个 SQL 查询，来删除 Person 表中所有重复的电子邮箱，重复的邮箱里只保留 Id 最小 的那个。

+----+------------------+
| Id | Email            |
+----+------------------+
| 1  | john@example.com |
| 2  | bob@example.com  |
| 3  | john@example.com |
+----+------------------+
Id 是这个表的主键。
例如，在运行你的查询语句之后，上面的 Person 表应返回以下几行:

+----+------------------+
| Id | Email            |
+----+------------------+
| 1  | john@example.com |
| 2  | bob@example.com  |
+----+------------------+

    spark.sql(
      """
        |select id,email from
        |(
        |select id,email,row_number() over(partition by email order by id) rk from homework_20200413.person
        |) tmp
        |where rk = 1
        |order by id,email
        |""".stripMargin).show()

9、
给定一个 Weather 表，编写一个 SQL 查询，来查找与之前（昨天的）日期相比温度更高的所有日期的 Id。

+---------+------------------+------------------+
| Id(INT) | RecordDate(DATE) | Temperature(INT) |
+---------+------------------+------------------+
|       1 |       2015-01-01 |               10 |
|       2 |       2015-01-02 |               25 |
|       3 |       2015-01-03 |               20 |
|       4 |       2015-01-04 |               30 |
+---------+------------------+------------------+
例如，根据上述给定的 Weather 表格，返回如下 Id:

+----+
| Id |
+----+
|  2 |
|  4 |
+----+

   spark.sql(
      """
        |select id from
        |(
        |select id,temperature-last_temperature newtemperature from
        |(
        |select id,recorddate,temperature,lag(temperature,1,50) over(order by recorddate) last_temperature from homework_20200413.weather
        |) tmp
        |) tmp2
        |where newtemperature > 0
        |""".stripMargin).show()

10、
这里有张 World 表

+-----------------+------------+------------+--------------+---------------+
| name            | continent  | area       | population   | gdp           |
+-----------------+------------+------------+--------------+---------------+
| Afghanistan     | Asia       | 652230     | 25500100     | 20343000      |
| Albania         | Europe     | 28748      | 2831741      | 12960000      |
| Algeria         | Africa     | 2381741    | 37100000     | 188681000     |
| Andorra         | Europe     | 468        | 78115        | 3712000       |
| Angola          | Africa     | 1246700    | 20609294     | 100990000     |
+-----------------+------------+------------+--------------+---------------+
如果一个国家的面积超过300万平方公里，或者人口超过2500万，那么这个国家就是大国家。

编写一个SQL查询，输出表中所有大国家的名称、人口和面积。

例如，根据上表，我们应该输出:

+--------------+-------------+--------------+
| name         | population  | area         |
+--------------+-------------+--------------+
| Afghanistan  | 25500100    | 652230       |
| Algeria      | 37100000    | 2381741      |
+--------------+-------------+--------------+

    spark.sql(
      """
        |select * from homework_20200413.world where area > 3000000 or population > 25000000
        |""".stripMargin).show()

11、
有一个courses 表 ，有: student (学生) 和 class (课程)。

请列出所有超过或等于5名学生的课。

例如,表:

+---------+------------+
| student | class      |
+---------+------------+
| A       | Math       |
| B       | English    |
| C       | Math       |
| D       | Biology    |
| E       | Math       |
| F       | Computer   |
| G       | Math       |
| H       | Math       |
| I       | Math       |
+---------+------------+
应该输出:

+---------+
| class   |
+---------+
| Math    |
+---------+
Note:
学生在每个课中不应被重复计算

    spark.sql(
      """
        |select class from homework_20200413.courses group by class having count(*) > 5
        |""".stripMargin).show()

12、
X 市建了一个新的体育馆，每日人流量信息被记录在这三列信息中：序号 (id)、日期 (date)、 人流量 (people)。

请编写一个查询语句，找出高峰期时段，要求连续三天及以上，并且每天人流量均不少于100。

例如，表 stadium：

+------+------------+-----------+
| id   | date       | people    |
+------+------------+-----------+
| 1    | 2017-01-01 | 10        |
| 2    | 2017-01-02 | 109       |
| 3    | 2017-01-03 | 150       |
| 4    | 2017-01-04 | 99        |
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+
对于上面的示例数据，输出为：

+------+------------+-----------+
| id   | date       | people    |
+------+------------+-----------+
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-08 | 188       |
+------+------------+-----------+
Note:
每天只有一行记录，日期随着 id 的增加而增加。

    spark.sql(
      """
        |select id,date,people from
        |(
        |select id,date,people,count(*) over(partition by sub_date) counts from
        |(
        |select id,date,people,date_sub(date,rk) sub_date from
        |(
        |select id,date,people,row_number() over(order by date) rk from homework_20200413.stadium where people >= 100) t1
        |)t2
        |)t3
        |where counts >= 3 order by date
        |""".stripMargin).show()

13、
某城市开了一家新的电影院，吸引了很多人过来看电影。该电影院特别注意用户体验，专门有个 LED显示板做电影推荐，上面公布着影评和相关电影描述。

作为该电影院的信息部主管，您需要编写一个 SQL查询，找出所有影片描述为非 boring (不无聊) 的并且 id 为奇数 的影片，结果请按等级 rating 排列。


例如，下表 cinema:
+---------+-----------+--------------+-----------+
|   id    | movie     |  description |  rating   |
+---------+-----------+--------------+-----------+
|   1     | War       |   great 3D   |   8.9     |
|   2     | Science   |   fiction    |   8.5     |
|   3     | irish     |   boring     |   6.2     |
|   4     | Ice song  |   Fantacy    |   8.6     |
|   5     | House card|   Interesting|   9.1     |
+---------+-----------+--------------+-----------+
对于上面的例子，则正确的输出是为：

+---------+-----------+--------------+-----------+
|   id    | movie     |  description |  rating   |
+---------+-----------+--------------+-----------+
|   5     | House card|   Interesting|   9.1     |
|   1     | War       |   great 3D   |   8.9     |
+---------+-----------+--------------+-----------+

    spark.sql(
      """
        |select * from homework_20200413.cinema where description != "boring" and id % 2 == 1 order by rating desc
        |""".stripMargin).show()

14、
小美是一所中学的信息科技老师，她有一张 seat 座位表，平时用来储存学生名字和与他们相对应的座位 id。
其中纵列的 id 是连续递增的
小美想改变相邻俩学生的座位。
你能不能帮她写一个 SQL query 来输出小美想要的结果呢？
示例：

+---------+---------+
|    id   | student |
+---------+---------+
|    1    | Abbot   |
|    2    | Doris   |
|    3    | Emerson |
|    4    | Green   |
|    5    | Jeames  |
+---------+---------+
假如数据输入的是上表，则输出结果如下：

+---------+---------+
|    id   | student |
+---------+---------+
|    1    | Doris   |
|    2    | Abbot   |
|    3    | Green   |
|    4    | Emerson |
|    5    | Jeames  |
+---------+---------+
注意：
如果学生人数是奇数，则不需要改变最后一个同学的座位。

    spark.sql(
      """
        |select (case when id%2=0 then id-1 when id%2!=0 and id=counts then id else id+1 end) as id,
        |student from
        |homework_20200413.students join
        |(select count(*) as counts from homework_20200413.students) tmp
        |order by id,student
        |""".stripMargin).show()

15、
给定一个 salary表，如下所示，有m=男性 和 f=女性的值 。交换所有的 f 和 m 值(例如，将所有 f 值更改为 m，反之亦然)。要求使用一个更新查询，并且没有中间临时表。
例如:
| id | name | sex | salary |
|----|------|-----|--------|
| 1  | A    | m   | 2500   |
| 2  | B    | f   | 1500   |
| 3  | C    | m   | 5500   |
| 4  | D    | f   | 500    |
运行你所编写的查询语句之后，将会得到以下表:

| id | name | sex | salary |
|----|------|-----|--------|
| 1  | A    | f   | 2500   |
| 2  | B    | m   | 1500   |
| 3  | C    | f   | 5500   |
| 4  | D    | m   | 500    |

    spark.sql(
      """
        |select id,name,if(sex="m","f","m"),salary from homework_20200413.salary
        |""".stripMargin).show()


 