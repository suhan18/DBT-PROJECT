from pyspark.sql import SparkSession

# 🔁 Update this with your exact .jar file path
mysql_jar_path = "/home/suhani/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar"

# ✅ Step 1: Create Spark session with MySQL connector
spark = SparkSession.builder \
    .appName("Traffic Analysis to MySQL") \
    .config("spark.jars", mysql_jar_path) \
    .getOrCreate()

# ✅ Step 2: Example DataFrame (replace this with your real one)
data = [
    ("2025-04-07 10:00", "MG Road", 10, 5, 15, 3, 2, 35),
    ("2025-04-07 11:00", "Brigade Road", 12, 7, 18, 2, 1, 40)
]
columns = ["Time", "Location", "Bike_count", "Auto_count", "Car_count", "Bus_count", "Truck_count", "Total_count"]

df = spark.createDataFrame(data, columns)

# ✅ Step 3: Write to MySQL
df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://172.25.80.1:3306/your_database_name") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "traffic_analysis") \
    .option("user", "root") \
    .option("password", "your_mysql_password") \
    .mode("append") \
    .save()
