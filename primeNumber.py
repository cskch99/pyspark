from pyspark import SparkContext
sc = SparkContext()

def partitionSize(it):
    s = 0
    for i in it:
        s += 1
    yield s


n = 10000
allnumbers = sc.parallelize(range(2, n), 8)
composite = allnumbers.flatMap(lambda x: range(x*2, n, x)).repartition(8)  #.repartition(8)
print (allnumbers.mapPartitions(partitionSize).collect()); 
print (composite.mapPartitions(partitionSize).collect()); 



prime = allnumbers.subtract(composite)
print(prime.sortBy(lambda b: b).collect())

sc.stop()
