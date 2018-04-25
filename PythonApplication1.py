from operator import add
from pyspark import SparkContext,SparkConf
 


from random import random
#from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
#    spark = SparkSession\
#        .builder\
#        .appName("PythonPi")\
#        .getOrCreate()


    conf = SparkConf().setAppName("PythonPi").setMaster("local[4]")
    sc = SparkContext(conf=conf)

    partitions = 20
    n = 10000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0


    rng= range(1,n+1)

    count = sc.parallelize(rng, partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    sc.stop()



#sc = SparkContext(appName="PythonWordCount")
#partitions = 1000
#sc.stop() 