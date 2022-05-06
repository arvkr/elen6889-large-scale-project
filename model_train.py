from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

from pyspark.ml.evaluation import BinaryClassificationEvaluator

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SQLContext(sc)

DATA_PATH = 'gs://6893_bucket/large-scale/project/dataset_accx.csv'
data = spark.read.csv(DATA_PATH, inferSchema=True, header=True)
dataset = data

stages = []
numericCols = ['ax1', 'ax2', 'ax3', 'ax4', 'ax5', 'ax6', 'ax7', 'ax8', 'ax9', 'ax10', 'ax11', 'ax12']
stages += [VectorAssembler(inputCols=numericCols, outputCol="features")]
rf = RandomForestClassifier(featuresCol='features', labelCol='activity', seed=100)
stages += [rf]

trainingData, testData = dataset.randomSplit(weights=[0.85, 0.15], seed=100)
pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(trainingData)
train_pred = pipelineModel.transform(trainingData)

predictions = pipelineModel.transform(testData)

p2 = PipelineModel.load("./rf/rfmodel")
predictions = p2.transform(testData)

print(predictions.take(1))

evaluator = BinaryClassificationEvaluator(labelCol="activity", rawPredictionCol="rawPrediction")
print('ROC: ', evaluator.evaluate(predictions))