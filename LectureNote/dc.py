#prefix sum algorithm

from pyspark import SparkContext
sc = SparkContext()


x = [1, 4, 3, 5, 6, 7, 0, 1]

rdd = sc.parallelize(x, 4).cache()

def f(iterator):
    s = 0
    for i in iterator:
        s += i
    yield s

sums = rdd.mapPartitions(f).collect()

print ( sums)

for i in range(1, len(sums)):
    sums[i] += sums[i-1]

print ( sums)

def g(index, iterator):
    global sums
    if index == 0:
        s = 0
    else:
        s = sums[index-1]
    for i in iterator:
        s += i
        yield s

prefix_sums = rdd.mapPartitionsWithIndex(g)
print (prefix_sums.collect())


#Maximum Subarray Problem
# Classical divide and conquer algorithm

import sys

A = [-3, 2, 1, -4, 5, 2, -1, 3, -1]

def MaxSubarray(A, p, r):
    if p == r:
        return A[p]
    q = (p+r)/2
    M1 = MaxSubarray(A, p, q)
    M2 = MaxSubarray(A, q+1, r)
    Lm = -sys.maxint
    Rm = Lm
    V = 0
    for i in range(q, p-1, -1):
        V += A[i]
        if V > Lm:
            Lm = V
    V = 0
    for i in range(q+1, r+1):
        V += A[i]
        if V > Rm:
            Rm = V
    return max(M1, M2, Lm+Rm)

print (MaxSubarray(A, 0, len(A)-1))

# Linear-time algorithm
# Written in a way so that we can call it for each partition

def linear_time(it):
    Vmax = -sys.maxsize
    V = 0
    for Ai in it:
        V += Ai
        if V > Vmax:
            Vmax = V
        if V < 0:
            V = 0
    yield Vmax
    
print (linear_time(A) )

# The Spark algorithm:

def compute_sum(it):
    yield sum(it)

def compute_LmRm(index, it):
    Lm = -sys.maxsize
    Rm = -sys.maxsize
    L = sums[index]
    R = 0
    for Ai in it:
        L -= Ai
        R += Ai
        if L > Lm:
            Lm = L
        if R > Rm:
            Rm = R
    yield (Lm, Rm)

num_partitions = 4
rdd = sc.parallelize(A, num_partitions).cache()
sums = rdd.mapPartitions(compute_sum).collect()
LmRms = rdd.mapPartitionsWithIndex(compute_LmRm).collect()
best = max(rdd.mapPartitions(linear_time).collect())

for i in range(num_partitions-1):
    for j in range(i+1, num_partitions):
        x = LmRms[i][0] + sum(sums[i+1:j]) + LmRms[j][1]
        if x > best:
            best = x

print (best)


