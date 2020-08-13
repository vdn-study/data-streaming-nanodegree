import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import *

def run_spark_job(spark):
    spark.sparkContext.setLogLevel("WARN")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka01-vn00c1.vn.infra:9092,kafka02-vn00c1.vn.infra:9092,kafka03-vn00c1.vn.infra:9092") \
        .option("subscribe", "streaming.itbi.topic_test_02") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10) \
        .load()

    df.printSchema()

    schema = StructType([
        StructField("crime_id", StringType(), True),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", StringType(), True),
        StructField("call_date", StringType(), True),
        StructField("offense_date", StringType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", StringType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True)
    ])
                                                                                      
    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    # distinct_table = (service_table
    #                   .select("original_crime_type_name", "disposition")
    #                   .distinct()
    #                  )

    query = service_table.writeStream.trigger(processingTime="10 seconds").format("console").option("truncate", "false").start()
    query.awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("StructuredStreamingSetup") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
