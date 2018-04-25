from pyspark import SparkContext
sc = SparkContext()


import threading
import random

partitions = 5
n = 5000000 * partitions

# use different seeds in different threads and different partitions
# a bit ugly, since mapPartitionsWithIndex takes a function with only index
# and it as parameters
def f1(index, it):
    random.seed(index + 987231)
    for i in it:
        x = random.random() * 2 - 1
        y = random.random() * 2 - 1
        yield 1 if x ** 2 + y ** 2 < 1 else 0

def f2(index, it):
    random.seed(index + 987232)
    for i in it:
        x = random.random() * 2 - 1
        y = random.random() * 2 - 1
        yield 1 if x ** 2 + y ** 2 < 1 else 0

def f3(index, it):
    random.seed(index + 987233)
    for i in it:
        x = random.random() * 2 - 1
        y = random.random() * 2 - 1
        yield 1 if x ** 2 + y ** 2 < 1 else 0
    
def f4(index, it):
    random.seed(index + 987234)
    for i in it:
        x = random.random() * 2 - 1
        y = random.random() * 2 - 1
        yield 1 if x ** 2 + y ** 2 < 1 else 0
    
def f5(index, it):
    random.seed(index + 987245)
    for i in it:
        x = random.random() * 2 - 1
        y = random.random() * 2 - 1
        yield 1 if x ** 2 + y ** 2 < 1 else 0

f = [f1, f2, f3, f4, f5]
    
# the function executed in each thread/job
def dojob(i):
    count = sc.parallelize(range(1, n + 1), partitions) \
              .mapPartitionsWithIndex(f[i]).reduce(lambda a,b: a+b)
    print ("Worker", i, "reports: Pi is roughly", 4.0 * count / n)

# create and execute the threads
threads = []
for i in range(5):
    t = threading.Thread(target=dojob, args=(i,))
    threads += [t]
    t.start()

print ("All started!")

# wait for all threads to complete
for t in threads:
    t.join()    

print ("All done!")

