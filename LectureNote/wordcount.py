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

if len(sys.argv) != 3:
    print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
    exit(-1)
    
sc = SparkContext(appName="PythonStreamingNetworkWordCount")

# Create a StreamingContext with batch interval of 5 seconds
ssc = StreamingContext(sc, 5)

# Create a DStream that will connect to hostname and port given by user
lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
