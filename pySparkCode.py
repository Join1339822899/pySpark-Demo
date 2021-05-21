from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('SparkCode').setMaster('local[*]')
sc = SparkContext(conf=conf)

lines = sc.textFile("C:\project\pySpark-Demo\Document\TestTxt.txt").flatMap(lambda x:x.split('.'))\
    .map(lambda x : (x,1))\
    .reduceByKey(lambda x,y : x+y).collect()

print(lines)