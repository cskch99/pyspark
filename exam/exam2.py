
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('MyApp').getOrCreate();

dfCustomer = spark.read.csv('exam/Customer.csv', header=True, inferSchema=True)
dfProduct = spark.read.csv('exam/Product.csv', header=True, inferSchema=True)
dfDetail = spark.read.csv('exam/SalesOrderDetail.csv', header=True, inferSchema=True)
dfHeader = spark.read.csv('exam/SalesOrderHeader.csv', header=True, inferSchema=True)



#sum all sales order
dfSumDetail = dfDetail.select('SalesOrderID', (dfDetail.UnitPrice * dfDetail.OrderQty
        * (1 - dfDetail.UnitPriceDiscount)).alias('netprice'))\
        .groupBy('SalesOrderID').sum('netprice') \
        .withColumnRenamed('sum(netprice)', 'TotalPrice')


#group it to customer id
salesData = dfHeader.join(dfSumDetail, 'SalesOrderID').groupBy('CustomerID')\
                .sum('TotalPrice').withColumnRenamed('sum(TotalPrice)', 'CustomerNetTotal')


#join with customer
dc = dfCustomer.join(salesData, 'CustomerID', 'left_outer')     \
    .groupBy('SalesPerson') \
    .sum('CustomerNetTotal')    

#show the result
dc.show();


# SELECT ProductID, Name, ListPrice 
# FROM Product 
# WHERE Color = 'black'

a = 1
if a == 0 :
    dfProduct.filter("Color = 'Black'")\
             .select('ProductID', 'Name', 'ListPrice')\
             .show(truncate=False)


    dfProduct.where(dfProduct.Color=='Black') \
             .select(dfProduct.ProductID, dfProduct['Name'], (dfProduct.ListPrice * 2).alias('DoublePrice')) \
             .show(truncate=False)

    dfProduct.where(dfProduct.ListPrice * 2 > 100) \
             .select(dfProduct.ProductID, dfProduct['Name'], dfProduct.ListPrice * 2) \
             .show(truncate=False)

