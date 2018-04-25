from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
 

class cluster:
    
#    pList = [] 
#    name = ""
    
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
        
    def printPoints(self):
        print (self.name+' Points:')
        print ('-----------------')
        for p in self.pList :
            print (str(p.x) +',' + str(p.y)) 
        print ('-----------------')
    

class dbscanner:
    
#    dataSet = []
#    count = 0
#    visited = []
#    member = []
#    Clusters = [] 

    def __init__(self):
        self.dataSet = []
        self.count = 0
        self.visited = []
        self.member = []
        self.Clusters = [] 


    def dbscan(self,splitIndex, iterator,eps,MinPts): 
        for i in iterator:
            for p in i[1]:
                self.dataSet.append(p);
        
        C = -1
        Noise = cluster('Noise' ) 
        for point in self.dataSet:
            if point not in self.visited:
                self.visited.append(point)
                NeighbourPoints = self.regionQuery(point,eps)
                
                if len(NeighbourPoints) < MinPts:
                    Noise.addPoint(point)
                else:
                    name = 'Cluster-' + str(splitIndex) + "-" + str(self.count);
                    C = cluster(name)
                    self.count+=1; 
                    self.expandCluster(point,NeighbourPoints,C,eps,MinPts)
                     
        self.Clusters.append(Noise);
        return  self.Clusters;
                    
            
    
    def expandCluster(self,point,NeighbourPoints,C,eps,MinPts):
        
        C.addPoint(point)
        
        for p in NeighbourPoints:
            if p not in self.visited:
                self.visited.append(p)
                np = self.regionQuery(p,eps)
                if len(np) >= MinPts:
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
        
        #C.printPoints()
        
                    
                
                     
    def regionQuery(self,P,eps):
        result = []
        for d in self.dataSet:
            if (d.distanceSquared(P)<=eps) :
                result.append(d)
        return result
    



class DBSCANPoint:
 
  def __init__(self, vector):
    self.x = vector[0]
    self.y = vector[1]

  def distanceSquared(self, other): 
    dx = other.x - self.x
    dy = other.y - self.y
    return (dx * dx) + (dy * dy)
   
 

class DBSCANRectangle :

  def __init__(self,x,y,x2,y2):
    self.x = x
    self.y = y
    self.x2 = x2
    self.y2 = y2

  def __eq__(self, other):
    return self.y == other.y and self.x == other.x
 
  def __hash__(self ):
    return hash((self.x,self.y))

# Returns whether other is contained by this box
  def contains(self, other) :
    return self.x <= other.x and other.x2 <= self.x2 and self.y <= other.y and other.y2 <= self.y2
 
  
# Returns whether point is contained by this box
  def containPoint(self, point ):  
    return self.x <= point.x and point.x <= self.x2 and self.y <= point.y and point.y <= self.y2 

# Returns a new box from shrinking this box by the given amount

  def shrink(self,amount) :
    return DBSCANRectangle(self.x + amount, self.y + amount, self.x2 - amount, self.y2 - amount)
 

  # Returns a whether the rectangle contains the point, and the point
  # is not in the rectangle's border
  def almostContains(self,point ) :
    self.x < point.x and point.x < self.x2 and self.y < point.y and point.y < self.y2
 
 

class DBSCANUtility:

  def __init__(self,eps):
        self.eps = eps

  def minimumRectangleSize(self) :
    return 2 * self.eps

  def corner(self, p) :
    return int ((self.shiftIfNegative(p) / self.minimumRectangleSize())) * self.minimumRectangleSize()

  def shiftIfNegative(self,p) :
    
    if (p < 0) :
        return p - self.minimumRectangleSize()
    else    :
        return p

  def toMinimumBoundingRectangle(self,vector) :
    point = DBSCANPoint(vector)
    x = self.corner(point.x)
    y = self.corner(point.y)
    rect = DBSCANRectangle(x, y, x + self.minimumRectangleSize(), y + self.minimumRectangleSize()) 
    return rect




 


class DBSCAN:
    
#    dataSet = [] 
#    partitions = []
#    labeledPartitionedPoints = []

##  def labeledPoints: RDD[DBSCANLabeledPoint] = {
##    labeledPartitionedPoints.values
##  }
    def extract(self, ca) :
        pt=ca[0]
        mg = ca[1]
        inner,main,outer = mg[0]
        if outer.containPoint(pt) :
            return True;
        else :
            return False;

    def firstItem(self, ca) :  
        for  x in ca  :
            return x;
 
    def clusterConvert(self, iterator, clusterMapping) :
        minCluster = min(list(iterator))
        if (minCluster == 'Noise') :
            return 'Noise';

        if (minCluster not in clusterMapping) :
            return minCluster;
 
        return clusterMapping[minCluster];

 
    def runLocalDBScan(self,splitIndex,points,eps, minPoints) :
        d = dbscanner();
        v = d.dbscan(splitIndex, points, eps, minPoints)
        return v;



    def train(self, vectors,eps, maxPointsPerPartition, minPoints) :

        util = DBSCANUtility(eps)
#generate the smallest rectangles that split the space
#and count how many points are contained in each one of them
        minimumRectanglesWithCount = vectors    \
            .map(lambda x: util.toMinimumBoundingRectangle(x))    \
            .map(lambda d: (d, 1))                        \
            .aggregateByKey((0), lambda x,y: x+y, lambda x,y: x+y)    \
#            .collect()                          

        print(minimumRectanglesWithCount.collect());

        minimumRectangleSize = eps * 2

 #       localPartitions = minimumRectanglesWithCount

#grow partitions to include eps
        localMargins = minimumRectanglesWithCount  \
            .map( lambda p: (p[0].shrink(eps), p[0], p[0].shrink(-eps)) ) \
            .zipWithIndex()

#        margins = vectors.context.broadcast(localMargins);

#find the best partitions for the data space
#        localPartitions = EvenSplitPartitioner.partition(minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize)
#        print("Found partitions: ")
##localPartitions.foreach(p => logDebug(p.toString))

        duplicated = vectors.map(lambda v: DBSCANPoint(v) ).cartesian(localMargins).filter( lambda ca: self.extract(ca))
        duplicated = duplicated.map(lambda v: (v[1][1], v[0]))


        clustered = duplicated.groupByKey().mapPartitionsWithIndex(lambda splitIndex, points : self.runLocalDBScan(splitIndex, points,eps, minPoints)).cache()


#find all candidate points for merging clusters and group them
#        mergePoints = clustered.flatMap({
#          case (partition, point) =>
#            margins.value
#              .filter({
#                case ((inner, main, _), _) => main.contains(point) && !inner.almostContains(point)
#              })
#              .map({
#                case (_, newPartition) => (newPartition, (partition, point))
#              })
#        }).groupByKey()

        flattened = clustered.flatMap(lambda xs: [( (x.x, x.y), xs.name) for x in xs.getPoints() ]);
        clusterResult = flattened.groupByKey().map(lambda xs: (xs[0], min(list(xs[1])) )).collect();
        fc = flattened.collect();
        gK = flattened.groupByKey().filter(lambda xs: len(xs[1]) > 1) 
        clusterMapping = gK.flatMap(lambda xs: [(x, self.firstItem(xs[1])) for x in xs[1] ]).groupByKey().map(lambda xs: (xs[0], min(list(xs[1])) )).collectAsMap();
        
#       duplicatedCluster = clustered.cartesian(clustered).filter(lambda ca: self.sameCluster( ca))
        clusterResult = flattened.groupByKey().map(lambda xs: (xs[0], self.clusterConvert(xs[1], clusterMapping) )).collect();
## print out the result
        collectCluster = clustered.collect();
#        for clus in collectCluster :
#            print ("--Partition Start: " + clus.name)
#            clus.printPoints();
#            print ("--Partition End: " + clus.name)


#        print ("--Merge Result: " + clus.name)
        for clus in clusterResult : 
            print (str(clus[0][0]) +"," + str(clus[0][1]) + "," + clus[1]); 

## print out the result


        return 0
