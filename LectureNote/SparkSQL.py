
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql import SparkSession

row = Row(name="Alice", age=11)
print row
print row['name'], row['age']
print row.name, row.age

row = Row(name="Alice", age=11, count=1)
print row.count
print row['count']

spark = SparkSession.builder.appName('MyApp').getOrCreate();
# Data file at https://www.cse.ust.hk/msbd/data/building.csv

df = spark.read.csv('LectureNote/building.csv', header=True, inferSchema=True)
# show the content of the dataframe
df.show()


# Print the dataframe schema in a tree format
df.printSchema()

# Create an RDD from the dataframe
dfrdd = df.rdd
dfrdd.take(3)

# Retrieve specific columns from the dataframe
df.select('BuildingID', 'Country').show()

from pyspark.sql.functions import *

df.where("Country='USA'").select('*', lit('OK')).show()

# Use GroupBy clause with dataframe 
df.groupBy('HVACProduct').count().show()

# Load data from csv files
# Data files at https://www.cse.ust.hk/msbd5003/data

dfCustomer = spark.read.csv('LectureNote/Customer.csv', header=True, inferSchema=True)
dfProduct = spark.read.csv('LectureNote/Product.csv', header=True, inferSchema=True)
dfDetail = spark.read.csv('LectureNote/SalesOrderDetail.csv', header=True, inferSchema=True)
dfHeader = spark.read.csv('LectureNote/SalesOrderHeader.csv', header=True, inferSchema=True)






# SELECT ProductID, Name, ListPrice 
# FROM Product 
# WHERE Color = 'black'

dfProduct.filter("Color = 'Black'")\
         .select('ProductID', 'Name', 'ListPrice')\
         .show(truncate=False)


dfProduct.where(dfProduct.Color=='Black') \
         .select(dfProduct.ProductID, dfProduct['Name'], (dfProduct.ListPrice * 2).alias('DoublePrice')) \
         .show(truncate=False)

dfProduct.where(dfProduct.ListPrice * 2 > 100) \
         .select(dfProduct.ProductID, dfProduct['Name'], dfProduct.ListPrice * 2) \
         .show(truncate=False)

