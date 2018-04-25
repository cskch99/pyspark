from pyspark import SparkContext
sc = SparkContext()

from pyspark.sql import Row

row = Row(name="Alice", age=11)
print (row)
print (row['name'], row['age'])
print (row.name, row.age)

row = (Row(name="Alice", age=11, count=1))
print (row.count)
print (row['count'])


