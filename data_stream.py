
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date


# TODO Create a schema for incoming resources
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
    StructField("common_location", StringType(), True),
])

# TODO create a spark udf to convert time to YYYYmmDDhh format
@psf.udf(StringType())
def udf_convert_time(timestamp):
    date = parse_date(timestamp)
    return str(date.strftime('%y%m%d%H'))

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "service-calls") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("startingOffsets", "earliest") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
                    .select(psf.from_json(psf.col('value'), schema)\
                    .alias("SERVICE_CALLS"))\
                    .select("SERVICE_CALLS.*")

    distinct_table = service_table.select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # TODO get different types of original_crime_type_name in 60 minutes interval
    counts_df = distinct_table\
                .withWatermark("call_datetime", "60 minutes")\
                .groupBy(psf.window(distinct_table.call_datetime, "10 minutes", "5 minutes"),distinct_table.original_crime_type_name)\
                .count()

    # TODO use udf to convert timestamp to right format on a call_date_time column
    converted_df = distinct_table\
                    .withColumn("call_datetime", udf_convert_time(distinct_table.call_datetime))

    # TODO apply aggregations using windows function to see how many calls occurred in 2 day span
    calls_per_2_days = converted_df\
                        .groupBy(psf.window(converted_df.call_datetime, "2 day"))\
                        .agg(psf.count("crime_id").alias("calls_per_2_day"))
                        .select("calls_per_2_day")
    # TODO write output stream 
    query = counts_df.writeStream\
            .outputMode('Complete')\
            .format('console').start()


    # TODO attach a ProgressReporter
    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark = SparkSession\
            .builder\
            .master("local")\
            .appName("SF Crime Stats")\
            .getOrCreate()
    spark.sparkContext.getConf().getAll()
    logger.info("Spark started")
    run_spark_job(spark)
    spark.stop()



