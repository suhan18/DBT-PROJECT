# To run this file use the command below (python trial_spark.py doesn't work for me)
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 sliding_spark.py

# Using the concept of Sliding Window to show the difference
# Doesn't give the best of the results

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum, when, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("VehicleDataSlidingWindowAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for Kafka data
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("vehicle", StringType()) \
    .add("count", IntegerType())

topic = "vehicle-data"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) # Important for window()

# Classify Vehicle types
vehicle_counts = parsed_df.withColumn("bike_count", when(col("vehicle") == "bike", col("count")).otherwise(0)) \
    .withColumn("car_count", when(col("vehicle") == "car", col("count")).otherwise(0)) \
    .withColumn("truck_count", when(col("vehicle") == "truck", col("count")).otherwise(0)) \
    .withColumn("auto_count", when(col("vehicle") == "auto", col("count")).otherwise(0)) \
    .withColumn("bus_count", when(col("vehicle") == "bus", col("count")).otherwise(0))

# Classify Regions
region_df = vehicle_counts.withColumn(
    "region",
    when(col("location").isin("Hebbal", "Yelahanka", "Jakkur"), "North")
    .when(col("location").isin("Jayanagar", "Banashankari", "JP Nagar"), "South")
    .when(col("location").isin("Whitefield", "Marathahalli", "Indiranagar"), "East")
    .otherwise("West")
)

# Group by Location, Region, and Sliding Window of 5 mins sliding every 2 mins
agg_df = region_df.groupBy(
    window(col("timestamp"), "5 seconds", "2 seconds"),
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
