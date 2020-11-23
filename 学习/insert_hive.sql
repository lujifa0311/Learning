--sqlserver to hive

--客户数据表
insert overwrite table ods_198_pd_as_user partition(create_day={date_key})
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
from as_user where deleted=0;

--交易明细表
insert overwrite table ods_198_pd_PaymentDetail partition(create_day={date_key})
select
DetailId,
OrderDate,
StoreNo,
OrderNo,
PaymentType,
Amount,
OrdPayCardHash
from PaymentDetail

--订单表
insert overwrite table ods_198_pd_OrdCusSrvDetail partition(create_day={date_key})
select
DetailId,
OrderDate,
StoreNo,
StoreName,
StoreOrderNo,
WebOrderNo,
OtherOrderNo,
OrderTime,
OrderSavedTime,
OrderActualTime,
OrderCompletedTime,
OrderSource,
DeliveryType,
OrderStatus,
CustomerName,
CustomerPhoneNumber,
CustomerCity,
CustomerPostalCode,
CustomerAddressName,
CustomerAddressNo,
DiscountType,
DiscountAmount,
OrderListAmount,
OrderSalesAmount,
OrderIdealFoodCost,
IsModified,
ModifiedDate,
ModifiedName,
AuthorizeName,
CancelName,
CancelReason,
DeliveryFee,
TakeTimeSec,
TakeTime,
LoadTimeSec,
LoadTime,
WaitTimeSec,
WaitTime,
OutTheDoorTimeSec,
OutTheDoorTime,
LegTimeSec,
LegTime,
RunTimeSec,
RunTime,
DOTSec,
DOT,
BearAmount,
SubsidyAmount,
OrderNetSalesAmount,
RegionDesc,
Order17NetAmount,
GIVEXDiscount,
OrderPhone,
WINGSFee,
CardAmount
from OrdCusSrvDetail

--地址类别表
insert overwrite table ods_198_pd_OrdAddressDetail partition(create_day={date_key})
select
OrderDate,
StoreNo,
StoreOrderNo,
RecipientPhone,
CustomerAddressName,
CustomerAddressNo,
StoreLatitude,
StoreLongitude,
Latitude,
Longitude,
Distance,
AddressType,
AddressTypeDetail,
custlatitude,
custlongitude,
updatedate 
from OrdAddressDetail

--优惠券明细表
insert overwrite table ods_198_pd_CouponDetail partition(create_day={date_key})
select
DetailId,
OrderDate,
RegionDesc,
StoreNo,
StoreName,
OrderNo,
OrderSource,
CouponId,
CouponDesc,
CouponQty,
DiscountAmount
from CouponDetail

--用户积分明细表
insert overwrite table ods_198_pd_as_user_point_record partition(create_day={date_key})
select
id,
user_id,
action,
value,
created_time,
reference
from as_user_point_record

--订单明细表(Pizza表)
insert overwrite table ods_198_pd_PizzaDetail partition(create_day={date_key})
select
DetailId,
OrderDate,
StoreNo,
OrderNo,
ItemSeq,
SalesTypeDesc,
SalesId,
SalesDesc,
PizzaComboId1,
PizzaComboDesc1,
PizzaComboId2,
PizzaComboDesc2,
PizzaSizeId,
PizzaSizeDesc,
PizzaDoughId,
PizzaDoughDesc,
PizzaSauceId,
PizzaSauceDesc,
AddToppingsId1,
AddToppingDesc1,
AddToppingsId2,
AddToppingDesc2,
LessToppingsId1,
LessToppingDesc1,
LessToppingsId2,
LessToppingDesc2,
PizOrderQty,
PizListAmount,
PizSalesAmount,
NoToppingPizCostAmount,
PizCostAmount,
CustomerPhoneNo1,
CustomerPhoneNo2,
PizNetSalesAmount
from PizzaDetail

--订单明细表(副食表)
insert overwrite table ods_198_pd_SideFoodDetail partition(create_day={date_key})
select
DetailId,
OrderDate,
StoreNo,
OrderNo,
ItemSeq,
SalesTypeDesc,
SalesId,
SalesDesc,
ProdCategoryDesc,
SideFoodId,
SideFoodDesc,
FlavorDesc,
SFOrderQty,
SFListAmount,
SFSalesAmount,
SFCostAmount,
CustomerPhoneNo1,
CustomerPhoneNo2,
SFNetSalesAmount
from SideFoodDetail

--用户优惠券使用明细表
insert overwrite table ods_198_pd_UserCoupon partition(create_day={date_key})
select
Rule_number,
Cellphone,
UserID,
RuleID,
Status,
Order_number,
Pos_number,
Rule_name,
expity_start,
expity_end,
create_time,
update_time
from UserCoupon

--产品维度表
insert overwrite table ods_198_pd_ProductsTextDump partition(create_day={date_key})
select
RecordType,
Version,
Location_Code,
Language_Code,
ProductCode,
ProdTextProductDesc,
ProdTextDisplayCode,
ProdTextSizeDesc,
ProdTextFlavorDesc,
ProdTextOptSelectGrpDesc,
ProdTextCategoryDesc,
ProdTextSizeShortDesc,
ProdTextFlavorShortDesc,
DatabaseVersion
from ProductsTextDump




insert overwrite table ods_198_pd_DM_Country_Info partition(create_day={date_key})
select
No，
CountryName,
ProvinceCode,
Province,
CityCode,
CityName,
CityNameAll,
UpdateTime
from Dim_Country_Info


insert overwrite table ods_198_pd_Products2Dump partition(create_day={date_key})
select
RecordType,
Version,
Location_Code,
ProductCode,
ProductUpdateUserCode,
ProductUpdateDate,
ProdCatCode,
ProdSizeCode,
ProdFlavorCode,
ProdOsgCode,
PrPrcTblCode,
TaxCatCode,
MakeLineId,
ProductIsActive,
ProductEffectiveDate,
ProductExpirationDate,
ProductDescText,
ProductShortDescText,
ProductPosDescText,
ProductBasePrice,
ProductSurchargeAmt,
ProductIsSurchargeOnlyOnProd1,
ProductBottleDepositAmt,
ProductMaxPrice,
ProductIsIncludedInRoyaltySales,
ProductIsTrackEmployeeSales,
ProductIsPrintLabel,
ProductWeightedEffort,
ProductLegacyCode,
ProductAdditionalWeight,
ProductSurchargeDescText,
PrPrcTblCode2,
ProductIsShortcut,
ProductShortcutDisplaySeq,
DatabaseVersion,
UpdateDate from Products2Dump


spark.catalog.setCurrentDatabase("cdp_prod")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.dynamic.partition=true")
spark.sql("set hive.exec.max.dynamic.partitions = 1000")
spark.sql("set mapreduce.input.fileinputformat.split.minsize=134217728")
spark.sql("set mapreduce.input.fileinputformat.split.maxsize=268435456")
spark.conf.set("spark.default.parallelism","80")
spark.conf.set("spark.sql.shuffle.partitions","80")
spark.conf.set("spark.sql.files.maxPartitionBytes","268435456")
spark.conf.set("spark.sql.files.openCostInBytes","134217728")
spark.conf.set("spark.yarn.executor.memoryOverhead","2048")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","52428800")
spark.conf.set("spark.sql.broadcastTimeout","300")
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize","10000")
spark.conf.set("spark.sql.adaptive.enabled","true")
spark.conf.set("spark.sql.crossJoin.enabled","true")
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize","134217728")
spark.conf.set("spark.sql.adaptive.minNumPostShufflePartitions","8")


val tablename="(select * from as_user u where u.deleted=0) temp"

val jdbcDF = spark.read.format("jdbc")
             .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
				.option("url", "jdbc:sqlserver://10.1.66.128:1433;DatabaseName=CRMDB_Read")
				.option("dbtable", tablename)
				.option("user", "CDPETL")
				.option("password", "JzjpqiXCoAl+/uUzfcmzs")
				.option("fetchsize", "5000")


confMysql.default.driver="com.microsoft.sqlserver.jdbc.SQLServerDriverr"

import java.util.Properties
val readConnProperties = new Properties()
readConnProperties.put("user", "CDPETL")
readConnProperties.put("password", "JzjpqiXCoAl+/uUzfcmzs")
readConnProperties.put("fetchsize","1000")
readConnProperties.put("Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
val mysqlUrl="jdbc:sqlserver://10.1.66.128:1433;DatabaseName=CRMDB_Read;UseCompression=True"
spark.read.jdbc(mysqlUrl,"OrdCusSrvDetail_Extension",readConnProperties).createOrReplaceTempView("OrdCusSrvDetail_Extension")
 
 
spark.read.format("jdbc").option("url", "jdbc:sqlserver://10.1.66.128:1433;DatabaseName=CRMDB_Read")
                         .option("dbtable", "OrdAddressDetail")
						 .option("user", "CDPETL")
						 .option("password", "JzjpqiXCoAl+/uUzfcmzs")
						 .option("fetchsize", "5000")
						 .load()
						 .createOrReplaceTempView("OrdCusSrvDetail_Extension")

val url: String = "jdbc:sqlserver://10.1.66.128:1433;DatabaseName=CRMDB_Read"
val table = "OrdCusSrvDetail_Extension"
val predicates =
  Array(
    "2014-01-01" -> "2015-01-01",
    "2015-01-01" -> "2016-01-01",
    "2016-01-01" -> "2017-01-01",
    "2017-01-01" -> "2018-01-01",
    "2018-01-01" -> "2019-01-01",
    "2019-01-01" -> "2020-01-01",
    "2020-01-01" -> "2021-01-01").map {case (start, end) =>s"cast(OrderDate as date) >= date '$start' " + s"AND cast(OrderDate as date) <= date '$end'"}
val properties: Properties = new Properties()
properties.setProperty("user","CDPETL")
properties.setProperty("password", "JzjpqiXCoAl+/uUzfcmzs")
properties.setProperty("Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
spark.read.jdbc(url, table,predicates, properties)











ods_198_pd_as_user




 select count(1) from ods_198_pd_ordcussrvdetail_extension p left outer join ods_198_pd_as_user o on o.create_day=20200825 and o.cell_phone<>'10000000000' and p.CustomerPhoneNumber=o.cell_phone  where p.create_day=20200820;