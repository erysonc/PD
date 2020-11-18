from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext

sc = SparkContext()
hc = HiveContext(sc)

hc.sql("DROP TABLE IF EXISTS TMP1_Dim_DATE")
hc.sql("DROP TABLE IF EXISTS TMP1_Dim_HOUR")
hc.sql("DROP TABLE IF EXISTS TMP2_Dim_DATE")
hc.sql("DROP TABLE IF EXISTS TMP2_Dim_HOUR")

hc.sql("CREATE TABLE IF NOT EXISTS TMP1_Dim_DATE as select distinct hash(cast(t1.MD as date)) as PK_DATE, cast(t1.MD as date) as Date, day(t1.MD) as Day, month(t1.MD) as month, year(t1.MD) as year, ((year(t1.MD)-2010)*2 + case when month(t1.MD) <=6 then 1 else 2 end) Semester from (select cast(at as timestamp) MD from STG_part union all select cast(current_date as timestamp) MD) t1 left join Dim_date on(hash(cast(t1.MD as date))=Dim_date.PK_DATE) where Dim_date.PK_DATE is null")
hc.sql("CREATE TABLE IF NOT EXISTS TMP1_Dim_HOUR as select distinct hash(t1.MD2) as PK_HOUR, t1.MD2 as Time, hour(t1.MD) as Hour, minute(t1.MD) as Minute, second(t1.MD) as secend, case when hour(t1.MD)<12 then 1 else 2 end as Shift from (select cast(at as timestamp) MD, substring(at,12,8) MD2  from STG_part) t1 left join Dim_HOUR on(hash(t1.MD2)=Dim_HOUR.PK_HOUR) where Dim_HOUR.PK_HOUR is null")

hc.sql("create table IF NOT EXISTS TMP2_Dim_DATE as select TFT.* from TMP1_Dim_DATE TFT left join Dim_DATE FT on(TFT.PK_DATE=FT.PK_DATE) where FT.PK_DATE is null") 
hc.sql("create table IF NOT EXISTS TMP2_Dim_HOUR as select TFT.* from TMP1_Dim_HOUR TFT left join Dim_HOUR FT on(TFT.PK_HOUR=FT.PK_HOUR) where FT.PK_HOUR is null") 

hc.sql("INSERT INTO Dim_DATE select TFT.* from TMP2_Dim_DATE TFT") 
hc.sql("INSERT INTO Dim_HOUR select TFT.* from TMP2_Dim_HOUR TFT") 

hc.sql("DROP TABLE IF EXISTS TMP1_FT_EQUIPMENT")
hc.sql("DROP TABLE IF EXISTS TMP1_FT_BROWSER")
hc.sql("DROP TABLE IF EXISTS TMP1_FT_CATEGORY ")
hc.sql("DROP TABLE IF EXISTS TMP2_FT_EQUIPMENT")
hc.sql("DROP TABLE IF EXISTS TMP2_FT_BROWSER")
hc.sql("DROP TABLE IF EXISTS TMP2_FT_CATEGORY")
hc.sql("DROP TABLE IF EXISTS TMP3_FT_EQUIPMENT")
hc.sql("DROP TABLE IF EXISTS TMP3_FT_BROWSER")
hc.sql("DROP TABLE IF EXISTS TMP3_FT_CATEGORY ")

hc.sql("CREATE TABLE TMP1_FT_EQUIPMENT as select hash(concat(platform, model,at)) as PK_EQUIPMENT, platform, model, at, count(1) QTD_ACESSOS_EQUIPMENT from STG_part group by platform, model, at")
hc.sql("CREATE TABLE TMP1_FT_BROWSER as select hash(concat(browser,at)) as PK_BROWSER, browser,at, count(1) QTD_ACESSOS_BROWSER from STG_part group by browser,at")
hc.sql("CREATE TABLE TMP1_FT_CATEGORY as select hash(concat(`page category`, `page category 1`,at)) as PK_CATEGORY, `page category`, `page category 1`, at, count(1) QTD_ACESSOS_CATEGORY from STG_part group by `page category`, `page category 1`, at")

hc.sql("create table TMP2_FT_EQUIPMENT as select PK_EQUIPMENT, platform, model, (case when substring(TFT.at,1,10) is null then -1 when DDT.Date is null then -3 else DDT.PK_Date end)as PK_Date_at,(case when substring(TFT.at,12,8) is null then -1 when DHR.Time is null then -3 else DHR.Shift end)as Shift_at,QTD_ACESSOS_EQUIPMENT from TMP1_FT_EQUIPMENT TFT LEFT JOIN Dim_Date DDT on(DDT.Date=substring(TFT.at,1,10)) LEFT JOIN Dim_Hour DHR on(DHR.Time=substring(TFT.at,12,8))")
hc.sql("create table TMP2_FT_BROWSER as select PK_BROWSER, browser, (case when substring(TFT.at,1,10) is null then -1 when DDT.Date is null then -3 else DDT.PK_Date end)as PK_Date_at,(case when substring(TFT.at,12,8) is null then -1 when DHR.Time is null then -3 else DHR.Shift end)as Shift_at,QTD_ACESSOS_BROWSER from TMP1_FT_BROWSER TFT LEFT JOIN Dim_Date DDT on(DDT.Date=substring(TFT.at,1,10)) LEFT JOIN Dim_Hour DHR on(DHR.Time=substring(TFT.at,12,8))")
hc.sql("create table TMP2_FT_CATEGORY as select PK_CATEGORY, `page category` as page_category, `page category 1` as page_category_1, (case when substring(TFT.at,1,10) is null then -1 when DDT.Date is null then -3 else DDT.PK_Date end)as PK_Date_at,(case when substring(TFT.at,12,8) is null then -1 when DHR.Time is null then -3 else DHR.Shift end)as Shift_at,QTD_ACESSOS_CATEGORY from TMP1_FT_CATEGORY TFT LEFT JOIN Dim_Date DDT on(DDT.Date=substring(TFT.at,1,10)) LEFT JOIN Dim_Hour DHR on(DHR.Time=substring(TFT.at,12,8))")

hc.sql("create table TMP3_FT_EQUIPMENT as select TFT.* from TMP2_FT_EQUIPMENT TFT left join FT_EQUIPMENT FT on(TFT.PK_EQUIPMENT=FT.PK_EQUIPMENT) where FT.PK_EQUIPMENT is null") 
hc.sql("create table TMP3_FT_BROWSER as select TFT.* from TMP2_FT_BROWSER TFT left join FT_BROWSER FT on(TFT.PK_BROWSER=FT.PK_BROWSER) where FT.PK_BROWSER is null") 
hc.sql("create table TMP3_FT_CATEGORY as select TFT.* from TMP2_FT_CATEGORY TFT left join FT_CATEGORY FT on(TFT.PK_CATEGORY=FT.PK_CATEGORY) where FT.PK_CATEGORY is null") 

hc.sql("INSERT INTO FT_EQUIPMENT select TFT.* from TMP3_FT_EQUIPMENT TFT") 
hc.sql("INSERT INTO FT_BROWSER select TFT.* from TMP3_FT_BROWSER TFT") 
hc.sql("INSERT INTO FT_CATEGORY select TFT.* from TMP3_FT_CATEGORY TFT") 
