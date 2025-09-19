from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("MarketTickConsumer") \
    .config("spark.jars.packages", 
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market-ticks") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str")

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", LongType()) \
    .add("timestamp", StringType())

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Stream to console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()