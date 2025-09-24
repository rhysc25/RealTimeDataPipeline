from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, ArrayType
from APIKeys import postgresPassword

spark = SparkSession.builder \
    .appName("MarketTickConsumer") \
    .config("spark.jars.packages", 
        "org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
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
    try: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/quantdb") \
        .option("dbtable", "ticks") \
        .option("user", "postgres") \
        .option("password", f"{postgresPassword}") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    except: pass

query = parsed_df.writeStream \
    .foreachBatch(saveToDB) \
    .outputMode("append") \
    .start()

query.awaitTermination()
