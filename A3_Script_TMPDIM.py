from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext

sc = SparkContext()
hc = HiveContext(sc)

hc.sql("DROP TABLE IF EXISTS TMP_Dim_Junk")
hc.sql("DROP TABLE IF EXISTS TMP_Dim_GeoReg_H")
hc.sql("DROP TABLE IF EXISTS TMP_Dim_Subjects_H")

hc.sql("CREATE TABLE TMP_Dim_Junk as SELECT hash(concat('StudentClient',T1.JV)) as SK_Junk, 'StudentClient' AS Junk_Type, T1.JV AS Junk_Value FROM (SELECT distinct StudentClient AS JV FROM STG_Sessions) AS T1")
hc.sql("INSERT INTO TMP_Dim_Junk SELECT hash(concat('PlanType',T1.JV)) as SK_Junk, 'PlanType' AS Junk_Type, T1.JV AS Junk_Value FROM (SELECT distinct PlanType AS JV FROM STG_Subscriptions) AS T1")
hc.sql("INSERT INTO TMP_Dim_Junk SELECT hash(concat('SignupSource',T1.JV)) as SK_Junk, 'SignupSource' AS Junk_Type, T1.JV AS Junk_Value FROM (SELECT distinct SignupSource AS JV FROM STG_students) AS T1")
hc.sql("CREATE TABLE TMP_Dim_GeoReg_H as select hash(concat(T1.State,T1.City)) as SK_GeoReg, T1.State, T1.City from (select distinct State, City from STG_students) as T1")
hc.sql("CREATE TABLE TMP_Dim_Subjects_H as SELECT hash(concat(T1.ID_Subjects, T1.NAME_Subjects, T1.ID_Courses, T1.NAME_Courses, T1.ID_Universities, T1.NAME_Universities)) as SK_Subjects, T1.ID_Subjects, T1.NAME_Subjects, T1.ID_Courses, T1.NAME_Courses, T1.ID_Universities, T1.NAME_Universities FROM(select distinct SBJ.ID as ID_Subjects, SBJ.Name as NAME_Subjects, CRS.ID as ID_Courses, CRS.Name as NAME_Courses, UNV.ID as ID_Universities, UNV.NAME as NAME_Universities from STG_students STD inner join STG_Student_Follow_Subject SFS on(STD.ID=SFS.StudentId) inner join STG_Subjects SBJ on(SBJ.ID=SFS.SubjectId) inner join STG_Courses CRS on(CRS.ID=STD.CourseId) inner join STG_Universities UNV on(UNV.ID=STD.UniversityId)) as T1")

