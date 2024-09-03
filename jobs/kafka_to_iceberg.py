from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from pyspark import SparkConf

LOG_LEVEL = 'INFO'

def write_to_iceberg(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        batch_df.writeTo("demo.my_db.test").append()


def main():
    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    #conf.set("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    conf.set("spark.sql.catalog.demo.uri", "http://rest:8181")
    conf.set("spark.sql.catalog.demo.warehouse", "s3a://warehouse/wh")
    conf.set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
    conf.set("spark.kafka.bootstrap.servers", "broker:29092")
    conf.set("spark.kafka.security.protocol", "PLAINTEXT")
    conf.set("spark.kafka.client.logger.level", LOG_LEVEL)

    spark = SparkSession.builder.appName("test").config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel(LOG_LEVEL)
    catalog_name = "demo"

    try:
        # This will use the catalog to list namespaces, verifying it's working
        namespaces = spark.sql(f"SHOW NAMESPACES IN {catalog_name}").collect()
        print(f"Successfully connected to Iceberg catalog '{catalog_name}'")
        print("Available namespaces:", [row[0] for row in namespaces])
    except Exception as e:
        print(f"Error connecting to Iceberg catalog: {str(e)}")

    # Create a new namespace if it doesn't exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.my_db")

    # Create the table
    spark.sql("CREATE TABLE IF NOT EXISTS demo.my_db.my_table (id INT, data STRING) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS demo.my_db.test (key STRING, value STRING) USING iceberg")
    # Insert data
    #spark.sql("INSERT INTO demo.my_db.my_table VALUES (1, 'test data')")
    # df = spark.sql("select * from demo.my_db.my_table")
    # df.show()
    # return

    # Create a SparkSession



    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "test") \
        .load()


    output_df = df.select(col("key").cast("string"), col("value").cast("string"))

    # Parse the value column as JSON (adjust if your data isn't JSON)
    #parsed_df = df.select(
    #    from_json(col("value").cast("string"), schema).alias("parsed_value")
    #)

    # Select the parsed columns
    #output_df = parsed_df.select("parsed_value.*")

    # Write the output to console (for demonstration)
    #query = output_df \
    #    .writeStream \
    #    .outputMode("append") \
    #    .format("console") \
    #    .start()

    query = output_df.writeStream.foreachBatch(write_to_iceberg).outputMode("append").option("checkPointLocation", "/tmp/checkPoint").start()
    query.awaitTermination()

if __name__ == "__main__":
    main()
