
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark import SparkContext
sc = SparkContext()


numPartitions = 10
lines = sc.textFile('./exam/adj_noun_pairs.txt', numPartitions)
pairs = lines.map(lambda l: tuple(l.split())).filter(lambda p: len(p)==2)
pairs.cache()

#group the tuple, and get the length
groupPair = pairs.groupBy(lambda f: (f[0], f[1])).map(lambda s: (s[0], len(list(s[1]))));
#order by decending and take the first ten line
result = groupPair.sortBy(lambda x: x[1], False).take(10);
#print the result
print(result)
