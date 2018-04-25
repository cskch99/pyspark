from pyspark import SparkContext
sc = SparkContext()


def partSize(it) : yield (len(list(it)))



n = 30000
def f(x):
    return x / (n/8)

d1 = range(0,n,16) 
d2 = range(0,n,8)

r1=sc.parallelize(zip(d1,d2), 8)
r1 = r1.partitionBy(8,f)
print (r1.mapPartitions(partSize).collect())

