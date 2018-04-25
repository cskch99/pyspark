 
 # ok feature end

 #ok I add a line
from pyspark import SparkContext
sc = SparkContext()

lines = sc.textFile('wasb://cluster@msbd.blob.core.windows.net/data/adj_noun_pairs.txt', sc.defaultParallelism)

lines.count()

lines.getNumPartitions()

lines.take(5)

# Converting lines into word pairs. 
# Data is dirty: some lines have more than 2 words, so filter them out.
pairs = lines.map(lambda l: tuple(l.split())).filter(lambda p: len(p)==2)
pairs.cache()

pairs.take(5)

N = pairs.count()

N

# Compute the frequency of each pair.
# Ignore pairs that not frequent enough
pair_freqs = pairs.map(lambda p: (p,1)).reduceByKey(lambda f1, f2: f1 + f2) \
                  .filter(lambda pf: pf[1] >= 100)

pair_freqs.take(5)

# Computing the frequencies of the adjectives and the nouns
a_freqs = pairs.map(lambda p: (p[0],1)).reduceByKey(lambda x,y: x+y)
n_freqs = pairs.map(lambda p: (p[1],1)).reduceByKey(lambda x,y: x+y)

a_freqs.take(5)

# Broadcasting the adjective and noun frequencies. 
a_dict = sc.broadcast(a_freqs.collectAsMap())
n_dict = sc.broadcast(n_freqs.collectAsMap())

from math import *

# Computing the PMI for a pair.
def pmi_score(pair_freq):
    w1, w2 = pair_freq[0]
    f = pair_freq[1]
    pmi = log(float(f)*N/(a_dict.value[w1]*n_dict.value[w2]), 2)
    return pmi, (w1, w2)

# Computing the PMI for all pairs.
scored_pairs = pair_freqs.map(pmi_score)

# Printing the most strongly associated pairs. 
scored_pairs.top(10)


sc.stop();