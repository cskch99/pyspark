from pyspark.sql.functions import *
 
from pyspark.sql import SparkSession
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


spark = SparkSession \
    .builder \
    .appName("aa") \
    .getOrCreate()

if len(sys.argv) != 3:
    print("Usage: network_wordcount.py <hostname> <port>")
    exit(-1)
     

lines = spark\
        .readStream\
        .format('socket')\
        .option('host', 'localhost')\
        .option('port', '9999')\
        .option('includeTimestamp', 'true')\
        .load()
        
# Split the lines into words, retaining timestamps
# split() splits each line into an array, and explode() turns the array into multiple rows
words = lines.select(explode(split(lines.value, ' ')).alias('word'),
                     lines.timestamp)
print(words);

windowedCounts = words.groupBy(
    window(words.timestamp, "20 seconds", "10 seconds"),
    words.word)\
    .count()

# Start running the query 
query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate', 'false')\
        .trigger(processingTime='20 seconds') \
        .start()

query.awaitTermination(120)
query.stop()
print "Finished"
