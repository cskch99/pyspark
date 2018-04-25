from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
import numpy as np
import sys
from operator import add
from math import sqrt
import time
import datetime

# a class to store on dbscan cluster
class DBCluster:
    
#define a cluster with a name and a list of DataPoint    
    def __init__(self,name):
        self.name = name
        self.pList = [] 
    
    def addPoint(self,point):
        self.pList.append(point) 

    def getPoints(self):
        return self.pList
    
    def erase(self):
        self.pList = []
    
    
    def has(self,point):
        
        if point in self.pList:
            return True
        
        return False
        

# a class to store each data point
class DataPoint:
    def __init__(self, X):
        self.X = X
        self.corePoint = False;
        self.expandPoint = False;

    def isCorePoint(self) :
        return self.corePoint;

    def setCorePoint(self) :
        self.corePoint = True;

    def setExpandPoint(self) :
        self.expandPoint = True;

    def getX(self):
        return self.X;

    def toString(self):
        content = "";
        for k in range(len(self.X)) :
            content += str(self.X[k]) + "," 
        if (self.corePoint) :
            content += "(Core)";

        return content;


    def distance(self, other): 
        d1 = 0
        for k in range(len(self.X)) :
            d1 += (self.X[k] - other.X[k]) * (self.X[k] - other.X[k])

        return sqrt(d1)
    
 
# the local db scan algorithm
class LocalDBScan:
    def __init__(self):
        self.dataSet = []
        self.count = 0
        self.visited = []
        self.member = []
        self.Clusters = [] 


    def dbscan(self,splitIndex, iterator,eps,MinPts): 
        self.dataSet = list(iterator);
        
        C = -1
        Noise = DBCluster('Noise' ) 
        for point in self.dataSet:
            if point not in self.visited:
                self.visited.append(point)
                NeighbourPoints = self.regionQuery(point,eps)
                
                if len(NeighbourPoints) < MinPts:
                    Noise.addPoint(point)
                else:
                    name = 'Cluster-' + str(splitIndex) + "-" + str(self.count);
                    C = DBCluster(name)
                    self.count+=1; 
                    self.expandCluster(point,NeighbourPoints,C,eps,MinPts)
                     
        self.Clusters.append(Noise);
        return  self.Clusters;
                    
    def parittionDbScan(self, splitIndex, iterator,eps, minPts) :
        return self.dbScan(self, splitIndex, list(iterator), eps, minPts);
            
    
    def expandCluster(self,point,NeighbourPoints,C,eps,MinPts):
        
        C.addPoint(point)
        
        for p in NeighbourPoints:
            if p not in self.visited:
                self.visited.append(p)
                np = self.regionQuery(p,eps)
                if len(np) >= MinPts:
                    p.setCorePoint();
                    for n in np:
                        if n not in NeighbourPoints:
                            NeighbourPoints.append(n)
                    
            for c in self.Clusters:
                if not c.has(p):
                    if not C.has(p):
                        C.addPoint(p)
                        
            if len(self.Clusters) == 0:
                if not C.has(p):
                    C.addPoint(p)
                        
        self.Clusters.append(C)
        
                      
    def regionQuery(self,P,eps):
        result = []
        for d in self.dataSet:
            if (d.distance(P)<=eps) :
                result.append(d)
        return result
    

class Box :
    def __init__(self, lowBound, upBound) :
        self.lowBound = list(lowBound)
        self.upBound = list(upBound)

    def getBound(self) :
        return (self.lowBound, self.upBound)

    def split(self, splitAxis, meanLoc) :
        lowBoundMean = list(self.lowBound);
        lowBoundMean[splitAxis] = meanLoc;

        upBoundMean = list(self.upBound);
        upBoundMean[splitAxis] = meanLoc;

        return (Box(self.lowBound, upBoundMean), Box(lowBoundMean, self.upBound))

    def containPoint(self, point):
        for k in range(len(self.lowBound)) :
            x = point.getX()
            if (x[k] < self.lowBound[k] or x[k] >= self.upBound[k]) :
                return False;

        return True;

    def toString(self) :
        desc = "(" ;
        for x in self.lowBound :
            desc += str(x) + ","

        desc += ")-("
        for x in self.upBound :
            desc += str(x) + ","
        desc += ")"
        return desc

    def expand(self, size) :
        lowBound = [x - size for x in self.lowBound] ;
        upBound = [x + size for x in self.upBound] ;
        return Box(lowBound,upBound)


class ExpandPartition:
    def __init__(self, originalPartition, expandSize, allDataPoints) :
        originalBox = originalPartition.getBox();
        newBox = originalBox.expand(expandSize);    
#find all point between newbox and originalBox
#        expandPoint = allDataPoints.filter(lambda p: newBox.containPoint(p) and not originalBox.containPoint(p));
        self.dataPoints = allDataPoints.filter(lambda p: newBox.containPoint(p) );
        self.dimension = originalPartition.getDimension();
        self.box = newBox;


    def toString(self) :
        desc = self.box.toString() + "\n";
        desc += "Count: " + str(self.dataPoints.count());
        return desc


class Partition:
    def __init__(self, box, dataPoints, dimension) :
        # O(n) to calculate the statistics of this partition
        self.dataPoints = dataPoints
        self.box = box;
        self.dimension = dimension;
        dim = dimension

        # Ensure all partition run together on the data points
        # calculate in parallel way the total, sum(x), sum(x*x)
        # using this three value to calculate the mean and variance
        temp = self.dataPoints.aggregate(np.zeros((3, dim)),
                                      lambda x,sc:  x + np.array(
                                          [np.ones(dim), sc.getX(), np.square(sc.getX()) ]),
                                      add)
        self.means = temp[1] / temp[0]
        self.variances = temp[2] / temp[0] - self.means * self.means;
        self.dataCount = temp[0][0]

    def getDimension(self):
        return self.dimension;


    def getDataCount(self):
        return self.dataCount;

    def getBox(self):
        return self.box;

    def findMeanLoc(self) :
        dim = self.dimension
        axis = np.argmax(self.variances)
        temp = self.dataPoints.aggregate(np.zeros((2, dim)),
                                      lambda x,sc:  x + np.array(
                                          [np.ones(dim), sc.getX() ]),
                                      add)
        means = temp[1] / temp[0]
        return means
 
    def split(self) :
        dim = self.dimension;

        # select a dimension with largest variance and hopefully this partition contains more than 1 cluster and prevent the merge in future stage
        splitAxis = np.argmax(self.variances)

        # split the partition by this axis and split in the mean location and hopefully it can split the parition more evenly
        meanLoc = self.means[splitAxis]
        splitBox = self.box.split(splitAxis, meanLoc);

        # find each point belong to which partition
        lowBoundDP = self.dataPoints.filter(lambda p: splitBox[0].containPoint(p));
        upBoundDP = self.dataPoints.filter(lambda p: splitBox[1].containPoint(p));
 
        # Create the partition and calculate statistics of this partiton
        return (Partition(splitBox[0], lowBoundDP, dim), Partition(splitBox[1], upBoundDP, dim  ));

    def toString(self) :
        desc = self.box.toString() + "\n";
        desc += "Count: " + str(self.dataPoints.count());
        return desc


class NoPartitionDBScan:

    def train(self, vectors,eps, minPoints) :

        points = vectors.map(lambda v: DataPoint(v)).collect();
        clusterResult = LocalDBScan().dbscan(0, points,eps, minPoints);

        return 0


class ParallelDBScan:
    def maxPerRow(a,b) :
        return np.max(a,b);

    def firstItem(self, ca) :  
        for  x in ca  :
            return x;

    def clusterConvert(self, iterator, clusterMapping) :
        minCluster = min(list(iterator))
#noise will not do cluster mapping
        if (minCluster == 'Noise') :
            return 'Noise';

        if (minCluster not in clusterMapping) :
            return minCluster;
 
        return clusterMapping[minCluster];

 
    def idxOfBiggestPartition(self, allPartitions) :
        idxOfBiggest = 0;
        biggestSize = 0;
        for x in range(len(allPartitions)) :
            if (allPartitions[x].getDataCount() > biggestSize) :
                biggestSize = allPartitions[x].getDataCount();
                idxOfBiggest = x;

        return idxOfBiggest;

    def boxContainPoint(self, ca):
        pt= ca[0];
        boxIdx = ca[1][0];
        box = ca[1][1];
        if (box.containPoint(pt)) :
            return True;

        return False;

    def boxesContains(self, boxes, pt) :
        boxId = 0;
        arrayOfResult = []
        for box in boxes :
            if (box.containPoint(pt)) :
                arrayOfResult.append((boxId, pt));

        return arrayOfResult;


    def niceSplitPartition(self, initialPartition, maxPartition) :
        allPartitions = [];
        allPartitions.append(initialPartition);

        maxSplitCount = maxPartition - 1;

        for k in range(maxSplitCount) :
 
            # find out the biggest partition
            partIdx = self.idxOfBiggestPartition(allPartitions);
            
            splitPartition = allPartitions[partIdx];

            # split this partition into two
            newPartition = splitPartition.split(); 

            # remove the partition
            allPartitions.pop(partIdx);

            # add back the two new parition
            allPartitions.append(newPartition[0]);
            allPartitions.append(newPartition[1]); 
        return allPartitions;


    def train(self, vectors,eps,  minPoints, maxPartition) :
        context = vectors.context;
        
        points = vectors.map(lambda v: DataPoint(v));
        dimension = len(vectors.first())

 
        # find out the range of the box, ~ O(n/p) p is # of executor
        maxmin=points.map(lambda n: ( n.getX(), n.getX())).reduce(lambda x,y: (np.maximum(x[0],y[0]), np.minimum(x[1],y[1]) ));

# now define the initial box
        box = Box(maxmin[1], maxmin[0]+0.1);
# now define the initial partition
        initialPartition = Partition(box, points, dimension) 

        # niceSplitPartition, ~ O(n/p* log(maxPartition) ) p is # of executor
        allPartitions = self.niceSplitPartition(initialPartition, maxPartition);
 
        expandBound = [];
        for s in range(len(allPartitions)) :
            aBox = allPartitions[s].getBox().expand(eps);
            expandBound.append(aBox);
 

        # broadcast all the broader to executor 
        bc_psBox = context.broadcast(expandBound);

        # find out each point belong to which box
        newPoints = points.flatMap(lambda ca: self.boxesContains( bc_psBox.value , ca)  );
 
        #shuffle and repartition the data by the partition we calculate
        data2 = newPoints.partitionBy(len(allPartitions)).map(lambda ca: ca[1]);
 
        # run db scan in each partition, use map parititonwithindex such that each partition know their index
        clustered = data2.mapPartitionsWithIndex(lambda  splitIndex, iterator:LocalDBScan().dbscan(splitIndex, iterator,eps, minPoints));

        # get back the result and their partition
        flattened = clustered.flatMap(lambda xs: [( tuple(x.getX()),xs.name ) for x in xs.getPoints() ]); 

        # cache the result for speedup
        flattened.cache();
 
        # check which partition has more than 1 (this probabily need to be merged)
        groupOfFlattened = flattened.groupByKey().filter(lambda xs: len(xs[1]) > 1) 

        # create a cluster mapping to store the relationship btw cluster
        clusterMapping = groupOfFlattened.flatMap(lambda xs: [(x, self.firstItem(xs[1])  ) for x in xs[1] ]).groupByKey().map(lambda xs: (xs[0], min(list(xs[1])) )).collectAsMap();

        # broadcast the mapping such that each parittion contain a read-only copy for speed up purpose
        bc_clusterMapping = context.broadcast(clusterMapping);

        # map the partition name again base on the broadcast variable
        clusterResult = flattened.groupByKey().map(lambda xs: (xs[0], self.clusterConvert(xs[1], bc_clusterMapping.value) )).collect();
 
        # return the result to client
        return clusterResult
