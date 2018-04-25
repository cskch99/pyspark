from pyspark import SparkContext 
from pyspark import SparkConf
from pyspark.sql import SparkSession
sc = SparkContext() 
spark = SparkSession.builder.appName('MyApp').getOrCreate();
 

sc.addPyFile("C:\\spark\\jars\\graphframes-0.3.0-spark2.0-s_2.11.jar")

from graphframes import *
from pyspark.sql.functions import *

# Vertics DataFrame
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 37),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 38),
  ("g", "Gabby", 60)
], ["id", "name", "age"])

# Edges DataFrame
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend"),
  ("g", "e", "follow")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(v, e)

g.vertices.show()
g.edges.show()

# g.vertices and g.edges are just DataFrames
# You can use any DataFrame API on them

e.filter("src = 'a'").show()

g.edges.filter("src = 'a'").count()

# A GraphFrame has additional attributes

g.outDegrees.show()

g.inDegrees.show()

g.inDegrees.explain()

myInDegrees = g.edges.groupBy('dst').count()\
               .withColumnRenamed('dst', 'id').withColumnRenamed('count', 'inDegree')
myInDegrees.show()

myInDegrees.explain()

print( g.inDegrees.storageLevel)

g.inDegrees.cache()

print( g.inDegrees.storageLevel)

print (g.vertices.storageLevel)

g.cache()

print (g.vertices.storageLevel)
print (g.edges.storageLevel)

# Count the number of followers of c.
# This queries the edge DataFrame.
print (g.edges.filter("relationship = 'follow' and dst = 'c'").count())

# A triplet view of the graph

g.triplets.show()

g.triplets.explain()

# Search for pairs of vertices with edges in both directions between them.
motifs = g.find("(a)-[]->(b); (b)-[]->(a)")
motifs.show()

# Find triangles

triangles = g.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")
triangles.show()

triangles.explain()

# Negation
oneway = g.find("(a)-[]->(b); !(b)-[]->(a)")
oneway.show()

# Find vertices without incoming edges. This is wrong:
g.find('!()-[]->(a)').show()
# Because negation is implemented as a subtraction

# Still doesn't work:
g.vertices.join(g.inDegrees, 'id').filter('inDegree=0').show()

# Why? Because inDegree is computed by a groupBy followed by a count
g.inDegrees.show()

# Correct way:
g.vertices.join(g.inDegrees, 'id', 'outer').filter('inDegree is null').show()

# Or use subtract:
g.vertices.select('id').subtract(g.inDegrees.select('id')).join(g.vertices,'id').show()

# More meaningful queries can be expressed by applying filters.
# Question: where is this filter applied?

g.find("(a)-[]->(b); (b)-[]->(a)").filter("b.age > 36").show()

# Find chains of 4 vertices such that at least 2 of the 3 edges are "friend" relationships.
# The when function is similar to the CASE WHEN in SQL

chain4 = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d)")

friendTo1 = lambda e: when(e['relationship'] == 'friend', 1).otherwise(0)

chain4.select('*',friendTo1(chain4['e1']).alias('f1'), \
                  friendTo1(chain4['e2']).alias('f2'), \
                  friendTo1(chain4['e3']).alias('f3')) \
      .where('f1 + f2 + f3 >= 2 AND a != d').select('a', 'b', 'c', 'd').show()

# Select subgraph of users older than 30, and edges of type "friend"
v2 = g.vertices.filter("age > 30")
e2 = g.edges.filter("relationship = 'friend'")
g2 = GraphFrame(v2, e2)

# GraphFrames does not check if a vertex is isolated (which is OK)
# or if an edge connects two existing vertices (which could cause bugs)
g2.vertices.show()

g2.inDegrees.show()

# Only keeping edges that connect existing vertices
e3 = e2.join(v2, e2['src'] == v2['id'], 'leftsemi') \
       .join(v2, e2['dst'] == v2['id'], 'leftsemi') 
g3 = GraphFrame(v2, e3)

g2.edges.show()

# Select subgraph based on edges of type "follow"
# pointing from an older user to an youner user.
e4 = g.find("(a)-[e]->(b)")\
      .filter("e.relationship = 'follow'")\
      .filter("a.age > b.age") \
      .select("e.*")

# Only keeping vertices that appear in the edges
v4 = g.vertices.join(e4, g.vertices['id'] == e4['src'], 'leftsemi') \
      .union(g.vertices.join(e4, g.vertices['id'] == e4['dst'], 'leftsemi')) \
      .distinct()
    
# Construct the subgraph
g4 = GraphFrame(v4, e4)
g4.vertices.show()

g.triplets.show()

# Starting vertex is 'a'
layers = [g.vertices.select('id').where("id = 'a'")]
visited =  layers[0]

while layers[-1].count() > 0:
    # From the current layer, get all the one-hop neighbors
    d1 = layers[-1].join(g.edges, layers[-1]['id'] == g.edges['src'])
    # Rename the column as 'id', and remove visited verices and duplicates
    d2 = d1.select(d1['dst'].alias('id')) \
           .subtract(visited).distinct()
    layers += [d2]
    visited = visited.union(layers[-1])

layers[0].show()

layers[1].show()

layers[2].show()

layers[3].show()

# GraphFrames provides own BFS:

paths = g.bfs("id = 'a'", "age > '36'")
paths.show()

import sys

g.edges.cache()
g.vertices.cache()
# Add the status and val column in the vertex dataframe
v = g.vertices.select('id', when(col('id')=='a', True).otherwise(False).alias('status'), 
                      when(col('id')=='a', 0).otherwise(sys.maxint).alias('val'))
while v.filter(v['status'] == True).count() > 0:
    newv = v.filter(v['status']==True).join(g.edges, v['id']==g.edges['src']) \
            .select(g.edges['dst'].alias('id'), (v['val']+1).alias('m')) \
            .groupBy('id').max('m').withColumnRenamed('max(m)', 'new_val')
                           # compute new_val
    v = v.join(newv, 'id', 'left_outer') \
         .select('id', when(newv['new_val'] < v['val'], newv['new_val'])
                 .otherwise(v['val']).alias('val'),
                 when(newv['new_val'] < v['val'], True)
                 .otherwise(False).alias('status'))
        # Spark SQL retains SQL's interpretation of NULL: 
        # Any operation involving NULL values returns NULL
        # when() only checks if the condition is True or not
    v.show()

# -1 denotes end of list
data = [(0, 5), (1, 0), (3, 4), (4, 6), (5, -1), (6,1)]
e = spark.createDataFrame(data, ['src', 'dst'])
v = e.select(col('src').alias('id'), when(e.dst == -1, 0).otherwise(1).alias('d'))
v1 = spark.createDataFrame([(-1, 0)], ['id', 'd'])
v = v.union(v1)

while e.filter('dst != -1').count() > 0:
    g = GraphFrame(v, e)
    v = g.triplets.select(col('src.id').alias('id'), 
                          (col('src.d') + col('dst.d')).alias('d')) \
         .union(v1)
    v.show()
    e = g.find('(a)-[]->(b); (b)-[]->(c)') \
         .select(col('a.id').alias('src'), col('c.id').alias('dst')) \
         .union(e.filter('dst = -1'))
    e.show()


