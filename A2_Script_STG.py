from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = HiveContext(sc)

vSTG_courses = sqlContext.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE A/courses.json")
vSTG_sessions = sqlContext.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE A/sessions.json")
vSTG_students = sqlContext.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE A/students.json")
vSTG_subjects = sqlContext.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE A/subjects.json")
vSTG_subscriptions = sqlContext.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE A/subscriptions.json")
vSTG_universities = sqlContext.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE A/universities.json")
vSTG_student_follow_subject = sqlContext.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE A/student_follow_subject.json")

vSTG_courses.registerTempTable("TMPSTG_courses")
vSTG_sessions.registerTempTable("TMPSTG_sessions")
vSTG_students.registerTempTable("TMPSTG_students")
vSTG_subjects.registerTempTable("TMPSTG_subjects")
vSTG_universities.registerTempTable("TMPSTG_universities")
vSTG_subscriptions.registerTempTable("TMPSTG_subscriptions")
vSTG_student_follow_subject.registerTempTable("TMPSTG_student_follow_subject")

sqlContext.sql("DROP TABLE IF EXISTS STG_courses")
sqlContext.sql("DROP TABLE IF EXISTS STG_sessions")
sqlContext.sql("DROP TABLE IF EXISTS STG_students")
sqlContext.sql("DROP TABLE IF EXISTS STG_subjects")
sqlContext.sql("DROP TABLE IF EXISTS STG_universities")
sqlContext.sql("DROP TABLE IF EXISTS STG_subscriptions")
sqlContext.sql("DROP TABLE IF EXISTS STG_student_follow_subject")

sqlContext.sql("CREATE TABLE STG_courses as select * from TMPSTG_courses")
sqlContext.sql("CREATE TABLE STG_sessions as select * from TMPSTG_sessions")
sqlContext.sql("CREATE TABLE STG_students as select * from TMPSTG_students")
sqlContext.sql("CREATE TABLE STG_subjects as select * from TMPSTG_subjects")
sqlContext.sql("CREATE TABLE STG_universities as select * from TMPSTG_universities")
sqlContext.sql("CREATE TABLE STG_subscriptions as select * from TMPSTG_subscriptions")
sqlContext.sql("CREATE TABLE STG_student_follow_subject as select * from TMPSTG_student_follow_subject")
