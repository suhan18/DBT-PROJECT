# To run this file use the command below (python trial_spark.py doesn't work for me)
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 trial_spark.py

# Using the concept of Micro Batching from Big Data which gives the most accurate results
# Micro batching is not exactly batch processing because what spark does it is it takes a batch of NEW data and processes it in a micro second

# How it works:
# Spark waits for data from Kafka for a micro second.
# All data received in that interval becomes 1 micro-batch.
# That micro-batch is processed just like a normal DataFrame.
# Result is output in almost real-time.



from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum, when, expr
from pyspark.sql.types import StructType, StringType, IntegerType

# This is just our Spark Session initialization
spark = SparkSession.builder \
    .appName("VehicleDataStreamingAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Schema for incoming Kafka data
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("vehicle", StringType()) \
    .add("count", IntegerType())


# This is the topic name we are using we need to include 2 more topics
topic = "vehicle-data"

# Get streaming data from Kafka using the same topic as producer
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .load()

# Parse value as JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# We want to find the count of each vehicle type
vehicle_counts = parsed_df.withColumn("bike_count", when(col("vehicle") == "bike", col("count")).otherwise(0)) \
    .withColumn("car_count", when(col("vehicle") == "car", col("count")).otherwise(0)) \
    .withColumn("truck_count", when(col("vehicle") == "truck", col("count")).otherwise(0)) \
    .withColumn("auto_count", when(col("vehicle") == "auto", col("count")).otherwise(0)) \
    .withColumn("bus_count", when(col("vehicle") == "bus", col("count")).otherwise(0))

# Also use when clause to classify the location into regions such as North, South, East, and West
region_df = vehicle_counts.withColumn(
    "region",
    when(col("location").isin("Hebbal", "Yelahanka", "Jakkur"), "North")
    .when(col("location").isin("Jayanagar", "Banashankari", "JP Nagar"), "South")
    .when(col("location").isin("Whitefield", "Marathahalli", "Indiranagar"), "East")
    .otherwise("West")
)

# Group by location & region only
agg_df = region_df.groupBy(
    col("location"),
    col("region")
).agg(
    spark_sum("bike_count").alias("total_bike_count"),
    spark_sum("car_count").alias("total_car_count"),
    spark_sum("truck_count").alias("total_truck_count"),
    spark_sum("auto_count").alias("total_auto_count"),
    spark_sum("bus_count").alias("total_bus_count"),
    (spark_sum("bike_count") + spark_sum("car_count") + spark_sum("truck_count") + spark_sum("auto_count") + spark_sum("bus_count")).alias("total_vehicle_count")
)

# Output to Console
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()