from operator import add
from pyspark import SparkContext,SparkConf
 


from random import random
#from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
 

    conf = SparkConf().setAppName("PyFileProcess").setMaster("local[4]")
    sc = SparkContext(conf=conf)

    txtFile = sc.textFile("./test.dat"); 
    totalLine = txtFile.count(); 
    print("Total Line %f" %  totalLine)

# no of line with len = 5
    lineWithLen5 = txtFile.filter(lambda p :  len(p) == 5);
    totalLine = lineWithLen5.count();
    print("Total Line with Len 5 = %d" %  totalLine)

# no of word with len = 5
    allWords = txtFile.flatMap(lambda x: x.split(" "));
    totalWordCount = allWords.count();
    print("Total word count = %d" %  totalWordCount)

    wordWithLen5 = allWords.filter(lambda p :  len(p) == 5).distinct();
    print("Total Line with Len 5 = %d" %   wordWithLen5.count())

    allWordWithLen5 = wordWithLen5.collect();
  
    sc.stop()

 