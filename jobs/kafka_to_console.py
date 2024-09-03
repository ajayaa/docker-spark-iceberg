from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from pyspark import SparkConf

LOG_LEVEL = 'INFO'

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("PySpark Kafka Example") \
        .config("spark.kafka.bootstrap.servers", "broker:29092") \
        .config("spark.kafka.security.protocol", "PLAINTEXT") \
        .config("spark.kafka.client.logger.level", LOG_LEVEL) \
        .getOrCreate()

    spark.sparkContext.setLogLevel(LOG_LEVEL)
    schema = StructType().add("value", StringType())

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "test") \
        .load()


    output_df = df.select(col("key").cast("string"), col("value").cast("string"))
    query = output_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
