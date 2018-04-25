from pyspark import SparkContext
sc = SparkContext()


from operator import add
lines = sc.textFile('README.md')
counts = lines.flatMap(lambda x: x.split()) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
topCounts = counts.sortBy(lambda x :x[2] )
print(topCounts.collect())