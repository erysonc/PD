from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext

sc = SparkContext()
hc = HiveContext(sc)

hc.sql("insert into Dim_junk select tmp.sk_junk, tmp.junk_type, tmp.junk_value from tmp_dim_junk tmp left join dim_junk dm on(dm.sk_junk=tmp.sk_junk) where dm.sk_junk is null")
print("Dim_junk")
hc.sql("insert into Dim_GeoReg_H select tmp.SK_GeoReg, tmp.State, tmp.City from TMP_Dim_GeoReg_H tmp left join Dim_GeoReg_H dm on(dm.SK_GeoReg=tmp.SK_GeoReg) where dm.SK_GeoReg is null")
print("Dim_GeoReg_H")
hc.sql("insert into Dim_Subjects_H select tmp.SK_Subjects, tmp.ID_Subjects, tmp.NAME_Subjects, tmp.ID_Courses, tmp.NAME_Courses, tmp.ID_Universities, tmp.NAME_Universities from TMP_Dim_Subjects_H tmp left join Dim_Subjects_H dm on(dm.SK_Subjects=tmp.SK_Subjects) where dm.SK_Subjects is null")
print("Dim_Subjects_H")

hc.sql("DROP TABLE IF EXISTS TMP1_Dim_DATE")
hc.sql("DROP TABLE IF EXISTS TMP1_Dim_HOUR")
hc.sql("DROP TABLE IF EXISTS TMP2_Dim_DATE")
hc.sql("DROP TABLE IF EXISTS TMP2_Dim_HOUR")

hc.sql("CREATE TABLE IF NOT EXISTS TMP1_Dim_DATE as select distinct hash(cast(t1.MD as date)) as PK_DATE, cast(t1.MD as date) as Date, day(t1.MD) as Day, month(t1.MD) as month, year(t1.MD) as year, ((year(t1.MD)-2010)*2 + case when month(t1.MD) <=6 then 1 else 2 end) Semester from (select cast(SessionStartTime as timestamp) MD from STG_sessions union all select cast(FollowDate as timestamp) MD from STG_Student_Follow_Subject union all select cast(RegisteredDate as timestamp) MD from STG_students union all select cast(current_date as timestamp) MD) t1 left join Dim_date on(hash(cast(t1.MD as date))=Dim_date.PK_DATE) where Dim_date.PK_DATE is null")
hc.sql("CREATE TABLE IF NOT EXISTS TMP1_Dim_HOUR as select distinct hash(t1.MD2) as PK_HOUR, t1.MD2 as Time, hour(t1.MD) as Hour, minute(t1.MD) as Minute, second(t1.MD) as secend, case when hour(t1.MD)<12 then 1 else 2 end as Shift from (select cast(SessionStartTime as timestamp) MD, substring(SessionStartTime,12,8) MD2 from STG_sessions union all select cast(FollowDate as timestamp) MD, substring(FollowDate,12,8) MD2 from STG_Student_Follow_Subject union all select cast(RegisteredDate as timestamp) MD, substring(RegisteredDate,12,8) MD2 from STG_students) t1 left join Dim_HOUR on(hash(t1.MD2)=Dim_HOUR.PK_HOUR) where Dim_HOUR.PK_HOUR is null")

hc.sql("create table TMP2_Dim_DATE as select TFT.* from TMP1_Dim_DATE TFT left join Dim_DATE FT on(TFT.PK_DATE=FT.PK_DATE) where FT.PK_DATE is null") 
hc.sql("create table TMP2_Dim_HOUR as select TFT.* from TMP1_Dim_HOUR TFT left join Dim_HOUR FT on(TFT.PK_HOUR=FT.PK_HOUR) where FT.PK_HOUR is null") 

hc.sql("INSERT INTO Dim_DATE select TFT.* from TMP2_Dim_DATE TFT") 
hc.sql("INSERT INTO Dim_HOUR select TFT.* from TMP2_Dim_HOUR TFT") 
