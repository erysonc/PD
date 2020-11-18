from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext

sc = SparkContext()
hc = HiveContext(sc)

hc.sql("CREATE TABLE IF NOT EXISTS Dim_Junk (SK_Junk INT, Junk_Type STRING, Junk_Value STRING)")
hc.sql("CREATE TABLE IF NOT EXISTS Dim_Subjects_H (SK_Subjects INT, ID_Subjects INT, NAME_Subjects STRING, ID_Courses INT, NAME_Courses STRING, ID_Universities INT, NAME_Universities STRING)")
hc.sql("CREATE TABLE IF NOT EXISTS Dim_GeoReg_H (SK_GeoReg INT, State STRING, City STRING)")
hc.sql("CREATE TABLE IF NOT EXISTS FT_Access (ID_StudentClient INT, ID_PlanType INT, ID_SignupSource INT, SK_GeoReg INT, SK_Subjects INT, PK_Date_SssStartTime INT, PK_Hour_SssStartTime INT, Semester INT, QTD_Students INT)")
hc.sql("CREATE TABLE IF NOT EXISTS FT_Std_Follow_Subj (SK_GeoReg INT, ID_SignupSource INT, SK_Subjects INT, PK_Date_FollowDate INT, PK_Hour_FollowDate INT, Semester INT, QTD_Students INT)")
--criar a tabela de data
--criar a tabela de hora

print("tabelas criadas com sucesso")

--inserir dados na tabela de data 
--inserir dados na tabela de hora
hc.sql("INSERT INTO Dim_Junk select  0, 'StudentClient','Outros'")
hc.sql("INSERT INTO Dim_Junk select -1, 'StudentClient','Não informado'")
hc.sql("INSERT INTO Dim_Junk select -2, 'StudentClient','Não se aplica'")
hc.sql("INSERT INTO Dim_Junk select -3, 'StudentClient','Não preenchido'")
hc.sql("INSERT INTO Dim_Junk select  0, 'PlanType','Outros'")
hc.sql("INSERT INTO Dim_Junk select -1, 'PlanType','Não informado'")
hc.sql("INSERT INTO Dim_Junk select -2, 'PlanType','Não se aplica'")
hc.sql("INSERT INTO Dim_Junk select -3, 'PlanType','Não preenchido'")
hc.sql("INSERT INTO Dim_Junk select  0, 'SignupSource','Outros'")
hc.sql("INSERT INTO Dim_Junk select -1, 'SignupSource','Não informado'")
hc.sql("INSERT INTO Dim_Junk select -2, 'SignupSource','Não se aplica'")
hc.sql("INSERT INTO Dim_Junk select -3, 'SignupSource','Não preenchido'")

hc.sql("INSERT INTO Dim_Subjects_H select  0, NULL, 'Outros', NULL, 'Outros', NULL, 'Outros')")
hc.sql("INSERT INTO Dim_Subjects_H select -1, NULL, 'Não informado' , NULL, 'Não informado' , NULL, 'Não informado')")
hc.sql("INSERT INTO Dim_Subjects_H select -2, NULL, 'Não se aplica' , NULL, 'Não se aplica' , NULL, 'Não se aplica')")
hc.sql("INSERT INTO Dim_Subjects_H select -3, NULL, 'Não preenchido', NULL, 'Não preenchido', NULL, 'Não preenchido')")

hc.sql("INSERT INTO Dim_GeoReg_H select  0, 'Outros', 'Outros')")
hc.sql("INSERT INTO Dim_GeoReg_H select -1, 'Não informado' , 'Não informado')")
hc.sql("INSERT INTO Dim_GeoReg_H select -2, 'Não se aplica' , 'Não se aplica')")
hc.sql("INSERT INTO Dim_GeoReg_H select -3, 'Não preenchido', 'Não preenchido')")

hc.sql("CREATE TABLE Dim_DATE as select distinct hash(cast(t1.MD as date)) as PK_DATE, cast(t1.MD as date) as Date, day(t1.MD) as Day, month(t1.MD) as month, year(t1.MD) as year, ((year(t1.MD)-2010)*2 + case when month(t1.MD) <=6 then 1 else 2 end) Semester from (select cast('2010-01-01 00:00:00' as timestamp) MD) t1")
hc.sql("CREATE TABLE Dim_HOUR as select distinct hash(t1.MD2) as PK_HOUR, t1.MD2 as Time, hour(t1.MD) as Hour, minute(t1.MD) as Minute, second(t1.MD) as secend, case when hour(t1.MD)<12 then 1 else 2 end as Shift from (select cast('2010-01-01 00:00:00' as timestamp) MD, substring('2010-01-01 00:00:00',12,8) MD2 ) t1")
