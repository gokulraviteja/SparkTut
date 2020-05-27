from pyspark.context import SparkContext
sc=SparkContext.getOrCreate()
#rdd=sc.textFile("file:///home/bellapukondar1/ggtext.csv")
rdd=sc.textFile("ggtext.csv")
print(type(rdd)) #Rdd



from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
prod=[(1,'a1','11'),(2,'a2','22'),(3,'a3','33'),(4,'a4','44'),(5,'a5','55'),(6,'a6','66'),(7,'a7','77'),(8,'a8','88'),(9,'a9','99'),(10,'a10','1010')]
dd=sc.parallelize(prod)
df=dd.toDF(['id','value','label'])
hive_context.registerDataFrameAsTable(df, "table1")

q="select * from table1"
out=hive_context.sql(q)
out.show()