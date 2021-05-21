from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, window
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .master("local[*]")\
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .option("includeTimestamp","true")\
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word"),lines.timestamp
)

# Generate running word count
wordCounts = words.withWatermark('timestamp','10 seconds').groupBy(
        window(words.timestamp, '10 seconds', '10 seconds'),
        words.word
    ).count().orderBy('window')

 # Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option('truncate', 'false')\
    .start()

query.awaitTermination()

