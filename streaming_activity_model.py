from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col, split
from pyspark.sql import functions as F

userSchema = StructType() \
            .add('date', 'date') \
            .add('time', 'timestamp') \
            .add('username', 'string') \
            .add('wrist', 'integer') \
            .add('activity', 'integer') \
            .add('acceleration_x', 'float') \
            .add('acceleration_y', 'float') \
            .add('acceleration_z', 'float') \
            .add('gyro_x', 'float') \
            .add('gyro_y', 'float') \
            .add('gyro_z', 'float') \
            .add('timestamp', 'timestamp')


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "my-stream") \
  .load() \
  .select(from_json(col("value").cast("string"), userSchema).alias('parsed_value'))

df2 = df.withColumn("date", split(df["parsed_value"].cast("string"), ", ").getItem(0))\
        .withColumn("time", split(df["parsed_value"].cast("string"), ", ").getItem(1))\
        .withColumn("u", split(df["parsed_value"].cast("string"), ", ").getItem(2))\
        .withColumn("w", split(df["parsed_value"].cast("string"), ", ").getItem(3))\
        .withColumn("a", split(df["parsed_value"].cast("string"), ", ").getItem(4))\
        .withColumn("ax", split(df["parsed_value"].cast("string"), ", ").getItem(5))\
        .withColumn("ay", split(df["parsed_value"].cast("string"), ", ").getItem(6))\
        .withColumn("az", split(df["parsed_value"].cast("string"), ", ").getItem(7))\
        .withColumn("gx", split(df["parsed_value"].cast("string"), ", ").getItem(8))\
        .withColumn("gy", split(df["parsed_value"].cast("string"), ", ").getItem(9))\
        .withColumn("gz", split(df["parsed_value"].cast("string"), ", ").getItem(10))\
        .withColumn("tstamp", split(df["parsed_value"].cast("string"), ", ").getItem(11))

df2 = df2.withColumn("pts", split(F.col("tstamp"), "}").getItem(0))
df2 = df2.withColumn("pts2", F.to_timestamp(F.col("pts"), "yyyy-MM-dd HH:mm:ss"))

df2.writeStream.format("console").outputMode("update").start()