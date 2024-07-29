# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("EventHubStream").getOrCreate()

# Event Hub connection configuration
connectionString = "Endpoint=sb://<NAMESPACE_NAME>.servicebus.windows.net/;SharedAccessKeyName=<SHARED_ACCESS_KEY_NAME>;SharedAccessKey=<SHARED_ACCESS_KEY>;EntityPath=<EVENT_HUB_NAME>"

# Read stream from Event Hubs
event_hub_config = {
    "eventhubs.connectionString": connectionString
}

df = spark.readStream.format("eventhubs").options(**event_hub_config).load()

# Define schema for the incoming data
schema = StructType([
    StructField("body", StringType(), True)
])

# Extract the body of the Event Hub messages
events_df = df.withColumn("body", col("body").cast("string"))

# Parse the JSON messages (if applicable)
# parsed_df = events_df.withColumn("data", from_json(col("body"), schema)).select("data.*")

# For this example, assume the body is a plain string (number)
parsed_df = events_df.selectExpr("cast(body as string) as number")

# Write the streaming data to console (for testing)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
