from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext

sc = SparkContext()
hc = HiveContext(sc)

hc.sql("drop table if exists TMP1_FT_Access")
hc.sql("drop table if exists TMP1_FT_Std_Follow_Subj")
hc.sql("drop table if exists TMP2_FT_Access")
hc.sql("drop table if exists TMP2_FT_Std_Follow_Subj")
hc.sql("drop table if exists TMP3_FT_Access")
hc.sql("drop table if exists TMP3_FT_Std_Follow_Subj")

hc.sql("create table TMP1_FT_Access as select SSS.StudentClient, SSS.SessionStartTime, SBS.PlanType, SFS.SubjectId, STD.SignupSource, STD.State, STD.City, STD.CourseId, STD.UniversityId, STD.RegisteredDate, count(1) QTD_Students from STG_Sessions SSS inner join STG_Students STD on(STD.ID=SSS.StudentId) inner join STG_Subscriptions SBS on(SBS.StudentId=STD.ID) inner join STG_Student_Follow_Subject SFS on(SFS.StudentId=STD.ID) group by SSS.StudentClient, SBS.PlanType, STD.SignupSource, STD.State,STD.City,SFS.SubjectId, STD.CourseId, STD.UniversityId,SSS.SessionStartTime,STD.RegisteredDate ")
print(" create table TMP1_FT_Access")
hc.sql("create table TMP1_FT_Std_Follow_Subj as select STD.State, STD.City, STD.SignupSource, SFS.SubjectId, STD.CourseId, STD.UniversityId, SFS.FollowDate, STD.RegisteredDate, count(1) QTD_Students from STG_Student_Follow_Subject SFS inner join STG_Students STD on(SFS.StudentId=STD.ID) group by STD.State,STD.City,STD.SignupSource,SFS.SubjectId,STD.CourseId,STD.UniversityId,SFS.FollowDate,STD.RegisteredDate ")
print(" create table TMP1_FT_Std_Follow_Subj")

hc.sql("create table TMP2_FT_Access as select (case when TMPFAC.StudentClient is null then -1 when STDCLN.Junk_Value is null then -3 else STDCLN.SK_Junk end)as ID_StudentClient, (case when TMPFAC.PlanType is null then -1 when PLNTYP.Junk_Value is null then -3 else PLNTYP.SK_Junk end)as ID_PlanType, (case when TMPFAC.SignupSource is null then -1 when SGNSRC.Junk_Value is null then -3 else SGNSRC.SK_Junk end)as ID_SignupSource, (case when TMPFAC.City is null then -1 when DGR.City is null or DGR.State is null then -3 else DGR.SK_GeoReg end)as SK_GeoReg, (case when TMPFAC.SubjectId is null or TMPFAC.CourseId is null or TMPFAC.UniversityId is null then -1 when DSB.ID_Subjects is null or DSB.ID_Courses is null or DSB.ID_Universities is null then -3 else DSB.SK_Subjects end)as SK_Subjects, (case when substring(TMPFAC.SessionStartTime,1,10) is null then -1 when DDT.Date is null then -3 else DDT.PK_Date end)as PK_Date_SssStartTime, (case when substring(TMPFAC.SessionStartTime,12,8) is null then -1 when DHR.Time is null then -3 else DHR.PK_Hour end)as PK_Hour_SssStartTime, (case when substring(TMPFAC.RegisteredDate,1,10) is null then -1 when DDT_SMT.Date is null then -3 else DDT_NOW.Semester-DDT_SMT.Semester end)as Semester, TMPFAC.QTD_Students as QTD_Students from TMP1_FT_Access TMPFAC LEFT JOIN Dim_Junk STDCLN on(STDCLN.Junk_Value=TMPFAC.StudentClient) LEFT JOIN Dim_Junk PLNTYP on(PLNTYP.Junk_Value=TMPFAC.PlanType) LEFT JOIN Dim_Junk SGNSRC on(SGNSRC.Junk_Value=TMPFAC.SignupSource) LEFT JOIN Dim_GeoReg_H DGR on(DGR.City=TMPFAC.City and DGR.State=TMPFAC.State) LEFT JOIN Dim_Subjects_H DSB on(DSB.ID_Subjects=TMPFAC.SubjectId and DSB.ID_Courses=TMPFAC.CourseId and DSB.ID_Universities=TMPFAC.UniversityId) LEFT JOIN Dim_Date DDT on(DDT.Date=substring(TMPFAC.SessionStartTime,1,10)) LEFT JOIN Dim_Hour DHR on(DHR.Time=substring(TMPFAC.SessionStartTime,12,8)) LEFT JOIN Dim_Date DDT_SMT on(DDT_SMT.Date=substring(TMPFAC.RegisteredDate,1,10)) LEFT JOIN Dim_Date DDT_NOW on(DDT_NOW.Date=current_date)  ")
print(" create table TMP2_FT_Access")
hc.sql("create table TMP2_FT_Std_Follow_Subj as select (case when TFT.City is null then -1 when DGR.City is null or DGR.State is null then -3 else DGR.SK_GeoReg end)as SK_GeoReg, (case when TFT.SignupSource is null then -1 when SGNSRC.Junk_Value is null then -3 else SGNSRC.SK_Junk end)as ID_SignupSource, (case when TFT.SubjectId is null or TFT.CourseId is null or TFT.UniversityId is null then -1 when DSB.ID_Subjects is null or DSB.ID_Courses is null or DSB.ID_Universities is null then -3 else DSB.SK_Subjects end)as SK_Subjects, (case when substring(TFT.FollowDate,1,10) is null then -1 when DDT.Date is null then -3 else DDT.PK_Date end) as PK_Date_FollowDate, (case when substring(TFT.FollowDate,12,8) is null then -1 when DHR.Time is null then -3 else DHR.PK_Hour end)as PK_Hour_FollowDate, (case when substring(TFT.RegisteredDate,1,10) is null then -1 when DDT_SMT.Date is null then -3 else DDT_NOW.Semester-DDT_SMT.Semester end) as Semester, TFT.QTD_Students from TMP1_FT_Std_Follow_Subj TFT LEFT JOIN Dim_GeoReg_H DGR on(DGR.City=TFT.City and DGR.State=TFT.State) LEFT JOIN Dim_Junk SGNSRC on(SGNSRC.Junk_Value=TFT.SignupSource) LEFT JOIN Dim_Subjects_H DSB on(DSB.ID_Subjects=TFT.SubjectId and DSB.ID_Courses=TFT.CourseId and DSB.ID_Universities=TFT.UniversityId) LEFT JOIN Dim_Date DDT on(DDT.Date=substring(TFT.FollowDate,1,10)) LEFT JOIN Dim_Hour DHR on(DHR.Time=substring(TFT.FollowDate,12,8)) LEFT JOIN Dim_Date DDT_SMT on(DDT_SMT.Date=substring(TFT.RegisteredDate,1,10)) LEFT JOIN Dim_Date DDT_NOW on(DDT_NOW.Date=current_date)")
print(" create table TMP2_FT_Std_Follow_Subj")

hc.sql("create table TMP3_FT_Access as select TFT.* from TMP2_FT_Access TFT left join FT_Access FT on(TFT.ID_StudentClient=FT.ID_StudentClient AND TFT.ID_PlanType=FT.ID_PlanType AND TFT.ID_SignupSource=FT.ID_SignupSource AND TFT.SK_GeoReg=FT.SK_GeoReg AND TFT.SK_Subjects=FT.SK_Subjects AND TFT.PK_Date_SssStartTime=FT.PK_Date_SssStartTime AND TFT.PK_Hour_SssStartTime=FT.PK_Hour_SssStartTime AND TFT.Semester=FT.Semester) where FT.ID_StudentClient is null") 
print(" create table TMP3_FT_Access")
hc.sql("create table TMP3_FT_Std_Follow_Subj as select TFT.* from TMP2_FT_Std_Follow_Subj TFT left join FT_Std_Follow_Subj FT on( TFT.SK_GeoReg=FT.SK_GeoReg AND TFT.ID_SignupSource=FT.ID_SignupSource AND TFT.SK_Subjects=FT.SK_Subjects AND TFT.PK_Date_FollowDate=FT.PK_Date_FollowDate AND TFT.PK_Hour_FollowDate=FT.PK_Hour_FollowDate AND TFT.Semester=FT.Semester) where FT.SK_GeoReg is null")  
print(" create table TMP3_FT_Std_Follow_Subj")

hc.sql("INSERT INTO FT_Access select TFT.* from TMP3_FT_Access TFT") 
print("INSERT INTO FT_Access")
hc.sql("INSERT INTO FT_Std_Follow_Subj select TFT.* from TMP3_FT_Std_Follow_Subj TFT")  
print("INSERT INTO FT_Std_Follow_Subj")

