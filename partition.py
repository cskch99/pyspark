from pyspark import SparkContext
sc = SparkContext()

a = sc.parallelize(zip(range(0,100), range(0,100)), 8)
b = sc.parallelize(zip(range(0,100), range(0,100)), 8)
print (a.partitioner)
a = a.reduceByKey(lambda x,y:x + 2*y)
#hash partition
print (a.partitioner.partitionFunc)
c = a.join(b)
print(c.collect());
print (c.partitioner.partitionFunc)



