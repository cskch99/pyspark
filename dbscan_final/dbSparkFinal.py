from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors

sc = SparkContext()
sc.addPyFile("./dbSparkFinalClass.py")
from dbSparkFinalClass import *

def main() :

    data = sc.textFile("./inputData.csv")

    parsedData = data.map(lambda s : Vectors.dense([float(i) for i in s.split(',')])).cache()
 

    dbScan = ParallelDBScan();
    trainResult = dbScan.train(parsedData, 3, 3,  4);

 
    for clus in trainResult : 
         print (str(clus[0][0]) +"," + str(clus[0][1]) + "," + clus[1]); 

    sc.stop()



main()
