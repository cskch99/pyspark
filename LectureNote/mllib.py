from pyspark import SparkContext 
from pyspark import SparkConf
from pyspark.sql import SparkSession
sc = SparkContext() 
spark = SparkSession.builder.appName('MyApp').getOrCreate();


from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Prepare training data from a list of (label, features) tuples.
# Dense Vectors are just NumPy arrays

training = spark.createDataFrame([
    (1, Vectors.dense([0.0, 1.1, 0.1])),
    (0, Vectors.dense([2.0, 1.0, -1.0])),
    (0, Vectors.dense([2.0, 1.3, 1.0])),
    (1, Vectors.dense([0.0, 1.2, -0.5]))], ["label", "features"])
# Create a LogisticRegression instance. This instance is an Estimator.
lr = LogisticRegression(maxIter=10, regParam=0.01)

# Print out the parameters, documentation, and any default values.
print( lr.explainParams())

# Learn a LogisticRegression model. This uses the parameters stored in lr.
model1 = lr.fit(training)

print (model1)
# model1 is a Model (i.e., a transformer produced by an Estimator)
print ("Model 1's trained coefficients: ", model1.coefficients)

# We may alternatively specify parameters using a Python dictionary as a paramMap
paramMap = {lr.maxIter: 20}
paramMap[lr.maxIter] = 30  # Specify 1 Param, overwriting the original maxIter.
paramMap.update({lr.regParam: 0.1, lr.threshold: 0.55})  # Specify multiple Params.

# You can combine paramMaps, which are python dictionaries.
paramMap[lr.probabilityCol] = "myProbability"  # Change output column name

# Now learn a new model using the paramMapCombined parameters.
# paramMapCombined overrides all parameters set earlier via lr.set* methods.
model2 = lr.fit(training, paramMap)
print ("Model 2's trained coefficients: ", model2.coefficients)

# Prepare test data
test = spark.createDataFrame([
    (1, Vectors.dense([-1.0, 1.5, 1.3])),
    (2, Vectors.dense([3.0, 2.0, -0.1])),
    (3, Vectors.dense([0.0, 2.2, -1.5]))], ["id", "features"])

# Make predictions on test data using the Transformer.transform() method.
# LogisticRegression.transform will only use the 'features' column.
# Note that model2.transform() outputs a "myProbability" column instead of the usual
# 'probability' column since we renamed the lr.probabilityCol parameter previously.

model1.transform(test).show()
model2.transform(test).show()

# Prepare training documents from a list of (id, text, label) tuples.
training = spark.createDataFrame([
    (0, "a b c d spark spark", 1),
    (1, "b d", 0),
    (2, "spark f g h", 1),
    (3, "hadoop mapreduce", 0)
], ["id", "text", "label"])

# A tokenizer converts the input string to lowercase and then splits it by white spaces.
tokenizer = Tokenizer(inputCol="text", outputCol="words")
tokenizer.transform(training).show()

# The same can be achieved by DataFrameAPI:
# But you will need to wrap it as a transformer to use it in a pipeline.

training.select('*', split(training['text'],' ').alias('words')).show()

# Maps a sequence of terms to their term frequencies using the hashing trick.
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
a = hashingTF.transform(tokenizer.transform(training))
a.show(truncate=False)

print (a.select('features').first())

# lr is an estimator
lr = LogisticRegression(maxIter=10, regParam=0.001)

# Now we are ready to assumble the pipeline
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
                            
# Fit the pipeline to training documents.
model = pipeline.fit(training)

# Prepare test documents, which are unlabeled (id, text) tuples.
test = spark.createDataFrame([
    (4, "spark i j k"),
    (5, "l m n"),
    (6, "spark hadoop spark"),
    (7, "apache hadoop")
], ["id", "text"])

# Make predictions on test documents and print columns of interest.
model.transform(test).show()

# Example showing a DAG pipeline

tokenizer = Tokenizer(inputCol="text", outputCol="words")

# Using two different hash functions to turn the words into vectors
hashingTF1 = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="feature1")
hashingTF2 = HashingTF(numFeatures = 1 << 10,
                       inputCol=tokenizer.getOutputCol(), outputCol="feature2")

# Combine two vectors into one.  VectorAssember is an transformer
combineFeature = VectorAssembler(inputCols=["feature1", "feature2"],
                                 outputCol="features")

lr = LogisticRegression(maxIter=10, regParam=0.001)

# Stages must be in topological order
pipeline = Pipeline(stages=[tokenizer, hashingTF1, hashingTF2, combineFeature, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)

# Make predictions on test documents and print columns of interest.
model.transform(test).show()

inspections = spark.read.csv('wasb://cluster@msbd.blob.core.windows.net/HdiSamples/HdiSamples/FoodInspectionData/Food_Inspections1.csv', inferSchema=True)

inspections.printSchema()

inspections.show()

# Drop unused columns and rename interesting columns.

# Keep interesting columns and rename them to something meaningful

# Mapping column index to name.
columnNames = {0: "id", 1: "name", 12: "results", 13: "violations"}
    
# Rename column from '_c{id}' to something meaningful.
cols = [inspections[i].alias(columnNames[i]) for i in columnNames.keys()]
   
# Drop columns we are not using.
df = inspections.select(cols).where(col('violations').isNotNull())

df.cache()
df.show()

df.take(1)

df.select('results').distinct().show()

df.groupBy('results').count().show()

# The function to clean the data

labeledData = df.select(when(df.results == 'Fail', 0)
                        .when(df.results == 'Pass', 1)
                        .when(df.results == 'Pass w/ Conditions', 1)
                        .alias('label'), 
                        'violations') \
                .where('label >= 0')

labeledData = cleanData(df)
    
labeledData.show()

trainingData, testData = labeledData.randomSplit([0.8, 0.2])

tokenizer = Tokenizer(inputCol="violations", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.01)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

model = pipeline.fit(trainingData)

predictionsDf = model.transform(testData)
predictionsDf.show()

numSuccesses = predictionsDf.where('label == prediction').count()
numInspections = predictionsDf.count()

print ("There were %d inspections and there were %d successful predictions" % (numInspections, numSuccesses))
print("This is a %d%% success rate" % (float(numSuccesses) / float(numInspections) * 100))

# We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
# This will allow us to jointly choose parameters for all Pipeline stages.
# A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
# We use a ParamGridBuilder to construct a grid of parameters to search over.
# With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
# this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.

paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 100, 1000]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=3)  

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(trainingData)

predictionsDf = cvModel.transform(testData)

numSuccesses = predictionsDf.where('label == prediction').count()
numInspections = predictionsDf.count()

print ("There were %d inspections and there were %d successful predictions" % (numInspections, numSuccesses))
print("This is a %d%% success rate" % (float(numSuccesses) / float(numInspections) * 100))

cvModel.explainParams()


