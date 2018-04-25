"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ spark-submit network_wordcount.py localhost 9999`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="PythonStreamingNetworkWordCount")


# Create a queue of RDDs
rdd = sc.textFile('./LectureNote/adj_noun_pairs.txt', 8)

# split the rdd into 5 equal-size parts
rddQueue = rdd.randomSplit([1,1,1,1,1], 123)
     
s= rddQueue[0].collect();
   
# Create a StreamingContext with batch interval of 5 seconds
ssc = StreamingContext(sc, 5)

# Feed the rdd queue to a DStream
lines = ssc.queueStream(rddQueue)

# Do word-counting as before
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Use transform() to access any rdd transformations not directly available in SparkStreaming
topWords = wordCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
topWords.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination(25)  # Wait for the computation to terminate
ssc.stop(False)
print("Finished")

