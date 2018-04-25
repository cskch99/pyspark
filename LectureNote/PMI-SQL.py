
from pyspark.sql.functions import *

lines = spark.read.text('wasb://cluster@msbd.blob.core.windows.net/data/adj_noun_pairs.txt')
lines.rdd.getNumPartitions()
lines.show()


# Converting lines into word pairs. 
# Data is dirty: some lines have more than 2 words, so filter them out.
# pairs = lines.map(lambda l: tuple(l.split())).filter(lambda p: len(p)==2)
words = lines.select(split(lines[0],' ').alias('w')).filter(size('w')==2) 
pairs = words.select(words['w'][0].alias('adj'), words['w'][1].alias('noun'))
pairs.cache()
pairs.show()

pairs.show()

N = pairs.count()

N

# Compute the frequency of each pair.
# Ignore pairs that not frequent enough
# pair_freqs = pairs.map(lambda p: (p,1)).reduceByKey(lambda f1, f2: f1 + f2) \
#                   .filter(lambda pf: pf[1] >= 100)

pair_freqs = pairs.groupBy('adj', 'noun').count().filter('count >= 100')

pair_freqs.show()

# Computing the frequencies of the adjectives and the nouns
# a_freqs = pairs.map(lambda p: (p[0],1)).reduceByKey(lambda x,y: x+y)
# n_freqs = pairs.map(lambda p: (p[1],1)).reduceByKey(lambda x,y: x+y)

a_freqs =  pairs.groupBy('adj').count().withColumnRenamed('count', 'adjcount')
n_freqs =  pairs.groupBy('noun').count().withColumnRenamed('count', 'nouncount')

a_freqs.show()

pair_freqs.join(a_freqs, 'adj').join(n_freqs, 'noun') \
          .select('adj', 'noun', 
                  log2(col('count')*N/(col('adjcount')*col('nouncount')))
                  .alias('PMI')) \
          .orderBy(desc('PMI')).show()


