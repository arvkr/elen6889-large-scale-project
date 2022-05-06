from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext

from pyspark.ml.feature import VectorAssembler

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


#Read csv file to dataframe
#=====your code here==========
BASE_PATH = 'gs://6893_bucket/large-scale/project/uci_dataset/'
TRAINx_DATA_PATH = BASE_PATH + 'train_X.csv'
TRAIN_DATA_PATH = BASE_PATH + 'train.csv'
TESTx_DATA_PATH = BASE_PATH + 'test_X.csv'
TEST_DATA_PATH = BASE_PATH + 'test.csv'

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SQLContext(sc)

trainx = spark.read.csv(TRAINx_DATA_PATH, inferSchema=True, header=True)
train = spark.read.csv(TRAIN_DATA_PATH, inferSchema=True, header=True)
testx = spark.read.csv(TESTx_DATA_PATH, inferSchema=True, header=True)
test = spark.read.csv(TEST_DATA_PATH, inferSchema=True, header=True)
#===============================

traincols = trainx.columns

assembler = VectorAssembler(inputCols=traincols,outputCol="features")

stages = []
stages += [assembler]
rf = RandomForestClassifier(featuresCol='features', labelCol='Activity', seed=100,maxDepth=13, numTrees=90)
stages += [rf]

pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(train)

# train = assembler.transform(train)
# test = assembler.transform(test)
# rfModel = pipelineModel.transform(train)

predictions = pipelineModel.transform(test)

# predictions.take(3)

evaluator = MulticlassClassificationEvaluator(labelCol="Activity", predictionCol="prediction",metricName = "accuracy")

print(evaluator.evaluate(predictions))

pipelineModel.save('gs://6893_bucket/large-scale/project/uci_rf')