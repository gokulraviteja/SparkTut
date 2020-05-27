from pyspark.sql import SparkSession
from pyspark.context import SparkContext
sc=SparkContext.getOrCreate()

spark = SparkSession.builder.enableHiveSupport().getOrCreate() # enable hivesupport is optional
#df=spark.read.csv("file:///home/bellapukondar1/ggtext.csv")
df=spark.read.csv("ggtext.csv")
print(type(df)) #Dataframe
df.show()

q="select * from tablename"
out=spark.sql(q)
data=out.collect()

from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
prod=[(1,'a1','11'),(2,'a2','22'),(3,'a3','33'),(4,'a4','44'),(5,'a5','55'),(6,'a6','66'),(7,'a7','77'),(8,'a8','88'),(9,'a9','99'),(10,'a10','1010')]
dd=sc.parallelize(prod)
df=dd.toDF(['id','value','label'])
hive_context.registerDataFrameAsTable(df, "table1")
q="select * from table1"
out=hive_context.sql(q)
out.show()

#writing to s3
df.write.save("s3://s3-........", format='csv', header=True)
df.write.mode("append").format("csv").save("s3://s3-emr-test-stg/crn_analysis/output_files/ab1.csv");