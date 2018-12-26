from pyspark.sql import SparkSession,SQLContext


filePath = "hdfs://master01:8020/pukang/201801.csv"

spark = SparkSession.builder.appName("test").master("yarn").getOrCreate()
df = spark.read.option("header" , "true").csv(filePath)
df.show()

df.printSchema()

df.select(df['商品名称']).show()

# count = df.groupBy('商品名称').count().collect()

df.createOrReplaceTempView('people')

sqlDF = spark.sql("select 商品名称,下单帐号 from people")
sqlDF.show()
