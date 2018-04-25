from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MyApp').getOrCreate();

numOfIterations = 10

lines = spark.read.text("Dblp.in")
# You can also test your program on the follow larger data set:
# lines = spark.read.text("dblp.in")

a = lines.select(split(lines[0],' '))
links = a.select(a[0][0].alias('src'), a[0][1].alias('dst'))
outdegrees = links.groupBy('src').count()
ranks = outdegrees.select('src', lit(1).alias('rank'))

 

for iteration in range(numOfIterations):
# FILL IN THIS PART
    ranks = links.join(ranks,'src')     \
        .join(outdegrees,'src')     \
        .select('dst', (ranks.rank / outdegrees['count']).alias('updateRatio')) \
        .withColumnRenamed('dst','src') \
        .groupBy('src') \
        .sum('updateRatio')     \
        .withColumnRenamed('sum(updateRatio)','rank')    \
        .select('src','rank')

    ranks = ranks.select('src', (0.15 + 0.85* ranks.rank).alias('rank'))


ranks.orderBy(desc('rank')).show()