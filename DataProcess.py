from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, ArrayType
from sqlalchemy import create_engine
from sqlalchemy import text
from APIKeys import mariaDBpassword, mariaDBIP

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
    .add("c", ArrayType(StringType())) \
    .add("p", DoubleType()) \
    .add("s", StringType()) \
    .add("t", LongType()) \
    .add("v", LongType())

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.drop("c")
parsed_df = parsed_df.withColumn("t", from_unixtime(col("t") / 1000))

def saveToDB(batch_df, batch_id):
    if batch_df.rdd.isEmpty(): # Converts to underlying apache RDD (Resilient Distributed Database)
        return

    # Write entire batch into one table using jdbc
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{mariaDBIP}/quantdb") \
        .option("dbtable", "ticks") \
        .option("user", "rhys") \
        .option("password", mariaDBpassword) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

query = parsed_df.writeStream \
    .foreachBatch(saveToDB) \
    .outputMode("append") \
    .start()

query.awaitTermination()
