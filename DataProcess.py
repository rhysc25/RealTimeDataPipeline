from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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

engine = create_engine("mysql+pymysql://rhys:"+mariaDBpassword+"@"+mariaDBIP+"/quantdb")

def saveToDB(batch_df, batch_id):
  
    pdf = batch_df.toPandas()

    if pdf.empty:
        return

    for symbol in pdf['s'].unique():
        table_name = f"{symbol}_Ticks"

        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    p FLOAT,
                    s VARCHAR(16),
                    t BIGINT NOT NULL,
                    v INT,
                    PRIMARY KEY (t)
                );
            """))

        # filter this symbol
        symbol_pdf = pdf[pdf['s'] == symbol]

        symbol_pdf.to_sql(table_name, engine, if_exists='append', index=False)

query = parsed_df.writeStream \
    .foreachBatch(saveToDB) \
    .outputMode("append") \
    .start()

query.awaitTermination()
