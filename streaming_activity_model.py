
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col, split
from pyspark.sql import functions as F

userSchema = StructType() \
            .add('acceleration_x1', 'float') \
            .add('acceleration_x2', 'float') \
            .add('acceleration_x3', 'float') \
            .add('acceleration_x4', 'float') \
            .add('acceleration_x5', 'float') \
            .add('acceleration_x6', 'float') \
            .add('acceleration_x7', 'float') \
            .add('acceleration_x8', 'float') \
            .add('acceleration_x9', 'float') \
            .add('acceleration_x10', 'float') \
            .add('acceleration_x11', 'float') \
            .add('acceleration_x12', 'float') \
            .add('activity', 'integer') \
            .add('timestamp', 'timestamp')


df = sqlContext \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "my-stream") \
  .load() \
  .select(from_json(col("value").cast("string"), userSchema).alias('parsed_value'))

df2 = df.withColumn("ax1", split(df["parsed_value"].cast("string"), ", ").getItem(0))\
        .withColumn("ax2", split(df["parsed_value"].cast("string"), ", ").getItem(1).cast("double"))\
        .withColumn("ax3", split(df["parsed_value"].cast("string"), ", ").getItem(2).cast("double"))\
        .withColumn("ax4", split(df["parsed_value"].cast("string"), ", ").getItem(3).cast("double"))\
        .withColumn("ax5", split(df["parsed_value"].cast("string"), ", ").getItem(4).cast("double"))\
        .withColumn("ax6", split(df["parsed_value"].cast("string"), ", ").getItem(5).cast("double"))\
        .withColumn("ax7", split(df["parsed_value"].cast("string"), ", ").getItem(6).cast("double"))\
        .withColumn("ax8", split(df["parsed_value"].cast("string"), ", ").getItem(7).cast("double"))\
        .withColumn("ax9", split(df["parsed_value"].cast("string"), ", ").getItem(8).cast("double"))\
        .withColumn("ax10", split(df["parsed_value"].cast("string"), ", ").getItem(9).cast("double"))\
        .withColumn("ax11", split(df["parsed_value"].cast("string"), ", ").getItem(10).cast("double"))\
        .withColumn("ax12", split(df["parsed_value"].cast("string"), ", ").getItem(11).cast("double"))\
        .withColumn("activity", split(df["parsed_value"].cast("string"), ", ").getItem(12))\
        .withColumn("tstamp", split(df["parsed_value"].cast("string"), ", ").getItem(13))

df2 = df2.withColumn("ax1-up", split(F.col("ax1"), "\{").getItem(1).cast("double"))
drop_cols = ("parsed_value", "ax1")
df2 = df2.drop(*drop_cols)
df2 = df2.withColumnRenamed("ax1-up", "ax1")

query = df2.writeStream.format("console").outputMode("update").option("truncate", False).start()

query.awaitTermination()