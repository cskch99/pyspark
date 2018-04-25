from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors

from dbSparkClass import *

def main() :
    sc = SparkContext()

    sc.addPyFile(".\\dbscan_spark\dbSparkClass.py")

    data = sc.textFile("./dbscan/abc.csv")

    parsedData = data.map(lambda s : Vectors.dense([float(i) for i in s.split(',')])).cache()
    #collectedData = parsedData.collect();
    print( "EPS: $eps minPoints: $minPoints")
#    print (collectedData)


    dbScan = DBSCAN();
    dbScan.train(parsedData, 9, 20, 5 );

    sc.stop()



main()
