from pyspark import Row
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()\

sc = spark.sparkContext

lines = sc.textFile("C:\project\pySpark-Demo\Document\TestTxt.txt").flatMap(lambda x:x.split('.'))\
    .map(lambda x : (x,1))\
    .reduceByKey(lambda x,y : x+y)

row = lines.map(lambda x : Row(x[0],x[1]))

schemaPeople = spark.createDataFrame(row).toDF('Key','Value') ##RDD转DataFrame
schemaPeople.createOrReplaceTempView("row")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT Key,Value FROM row order by Value desc")
teenagers.show()
#
# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.Key).collect()#DataFrame转RDD
for name in teenNames:
    print(name)

