from pyspark import SparkContext 
sc = SparkContext()


numPartitions = 10

lines = sc.textFile('./quiz/adj_noun_pairs.txt', numPartitions)
pairs = lines.map(lambda l: tuple(l.split())).filter(lambda p: len(p)==2)
#pairs.cache()

#collectThePairs = pairs.collect();
# FILL IN YOUR CODE HERE 
#print (pairs.first()[0])
#print (pairs.first()[1])


# use SparkSQL
#print(pairs.filter(lambda l: l[1]=="unification").first() )
 

#either return -1 or return the index with value
def findFirst(iterator) :
    found = -1
    adjWordPair = ("","")
    for i in iterator :
        word = i[0][1];
        idx = i[1]; 
        if (word  ==  "unification") :
            found = idx
            adjWordPair =  i[0] 
            break

    yield found, adjWordPair


#add a index to the raw data and start partition
x = pairs.zipWithIndex().mapPartitions(findFirst)
 
#merge the partition result and get the one with smallest index
mergePartition = x.filter(lambda r: r[0] != -1).takeOrdered(1, lambda r: r[0] );

#print out the result
if (len(mergePartition) == 0) :
    print ("Not found")
else :
    print (mergePartition[0][1]);
  
sc.stop()



