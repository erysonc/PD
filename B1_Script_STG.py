from pyspark import SparkContext, HiveContext
from pyspark.sql import SQLContext

sc = SparkContext()
hc = HiveContext(sc)

vSTG_part = hc.read.json("file:///mnt/teste1/Datasets - Teste Data Engineer - Passei Direto/BASE B/part-*.json")

vSTG_part.registerTempTable("TMPSTG_part")

hc.sql("DROP TABLE IF EXISTS STG_part")

hc.sql("CREATE TABLE STG_part as select * from TMPSTG_part")

hc.sql("CREATE TABLE IF NOT EXISTS FT_EQUIPMENT (PK_EQUIPMENT INT, platform STRING, model STRING,PK_Date_at INT,Shift_at INT,QTD_ACESSOS_EQUIPMENT INT)")
hc.sql("CREATE TABLE IF NOT EXISTS FT_BROWSER (PK_BROWSER INT, browser STRING,PK_Date_at INT,Shift_at INT,QTD_ACESSOS_EQUIPMENT INT)")
hc.sql("CREATE TABLE IF NOT EXISTS FT_CATEGORY (PK_CATEGORY INT, page_category STRING, page_category_1 STRING,PK_Date_at INT,Shift_at INT,QTD_ACESSOS_EQUIPMENT INT)")
