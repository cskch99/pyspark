from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession.builder.appName('MyApp').getOrCreate();

from operator import add

df = spark.read.csv('sales.csv', header=True, inferSchema=True)

distinctCountry = df.select('Country').distinct()
#display it
distinctCountry.show()

filterRec = df.filter("Country = 'Brazil'").select('Name','Price')
#display it
filterRec.show()

countryPrice = df.groupBy('Country').sum('Price').withColumnRenamed('sum(Price)', 'TotalPrice').select('Country','TotalPrice')
#display it
countryPrice.show()


countryPrice = df.groupBy('Country').sum('Price').withColumnRenamed('sum(Price)', 'TotalPrice').select('Country','TotalPrice').orderBy('TotalPrice', ascending=False)
#display it
countryPrice.show()

df2 = spark.read.csv('countries.csv', header=True, inferSchema=True)

countryIdPrice = df.groupBy('Country').sum('Price').withColumnRenamed('sum(Price)', 'TotalPrice').join(df2,'Country').select('ID','TotalPrice')
#display it
countryIdPrice.show()
