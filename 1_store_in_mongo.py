"""This code is using apachey spark to read and store the ProjectTweets data into MongoDB.
Local_host :- mongodb://localhost:27017
DB_name :- Big_Tweet
Collection_Name :- Tweets_Data
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, StringType, TimestampType
import os
import sys

# Manually set Spark and Java home directory
spark_home = "C:/Spark"
java_home = "C:/Java"

# Set environment variables
os.environ['SPARK_HOME'] = spark_home
os.environ['JAVA_HOME'] = java_home
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 4g --executor-memory 4g pyspark-shell'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = "notebook"
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_PYTHON'] = sys.executable

# Add the bin directory to the PATH
os.environ['PATH'] = os.path.join(spark_home, 'bin') + os.pathsep + os.environ['PATH']

spark = SparkSession.builder \
    .appName('practice') \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

# Define the custom header based on your DataFrame's schema
custom_header = StructType([
    StructField("_id", StringType(), True), StructField("ids", LongType(), True),
    StructField("date", StringType(), True), StructField("flag", StringType(), True),
    StructField("user", StringType(), True), StructField("text", StringType(), True),
])

# Read data from a CSV file into a DataFrame with the custom header
df = spark.read.csv("ProjectTweets.csv", header=False, schema=custom_header)

# Drop the unnecessary index column "_id" if you don't need it
df = df.drop("_id")

# Rename columns to match MongoDB structure
df = df.withColumnRenamed("ids", "ids")
df = df.withColumnRenamed("_c2", "date")
df = df.withColumnRenamed("_c3", "flag")
df = df.withColumnRenamed("_c4", "user")
df = df.withColumnRenamed("_c5", "text")

# Write the DataFrame to MongoDB using the MongoDB Spark Connector
df.write.format("mongo").mode("append") \
    .option("uri", "mongodb://localhost:27017/Big_Tweet") \
    .option("collection","Tweets_Data").save()

print("Data written to MongoDB successfully!")