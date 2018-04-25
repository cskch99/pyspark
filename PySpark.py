from pyspark import SparkContext
sc = SparkContext()


A = sc.parallelize(range(10))
print (A.collect());
x = 5

B = A.filter(lambda z: z< x);
print (B.count());
x = 3
print (B.count());



p1 = sc.parallelize([(1,'Ben'), (2,'Ken')]);
p2 = sc.parallelize([(1, (3, 'RND')), (2, (5, 'PE')), (3, (6,'UN'))]);

print (p1.join(p2).collect());
print (p2.join(p1).collect());

#broadcast variable keep read only variable cache on each machine, more efficient than sending closures to tasks
b_p2 = sc.broadcast(p2.collectAsMap()); 

print (b_p2.value)
print (p1.map(lambda x : (x[0], b_p2.value[x[0]], x[1])).collect())
