import re
from operator import add
from pyspark import SparkContext,SparkConf
 


from random import random
#from operator import add

from pyspark.sql import SparkSession

def splitLine(l) :
    spLine = l.split(' ');
    return spLine[0], spLine[1]

def computeContribs(urls, rank):
    # Calculates URL contributions to the rank of other URLs.
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


if __name__ == "__main__":
 

    conf = SparkConf().setAppName("PageRank").setMaster("local[4]")
    sc = SparkContext(conf=conf)

    txtFile = sc.textFile("./pagerank_data.txt"); 
    totalLine = txtFile.count(); 
    print("Total Line %f" %  totalLine)
 
    lineMap = txtFile.map(lambda l: splitLine(l)).groupByKey();
#    print (lineMap)
    ranks = lineMap.map(lambda url_neighbors: (url_neighbors[0], 1.0))
#    print (ranks.collect())

    
    numOfIterations = 10

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(numOfIterations):
        # Calculates URL contributions to the rank of other URLs.
        contribs = lineMap.join(ranks, lineMap.getNumPartitions()) \
                        .flatMap(lambda u:
                                 computeContribs(u[1][0],
                                                 u[1][1]))
        # After the join, each element in the RDD is of the form
        # (url, (sequence of neighbor urls, rank))
    
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    print (ranks.sortBy(lambda s: s[1], False).take(1000))

    sc.stop()

 