# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark Session
spark = SparkSession.builder.appName("EventHubStream").getOrCreate()

# Event Hub connection configuration
connectionString = "Endpoint=sb://<NAMESPACE_NAME>.servicebus.windows.net/;SharedAccessKeyName=<SHARED_ACCESS_KEY_NAME>;SharedAccessKey=<SHARED_ACCESS_KEY>;EntityPath=<EVENT_HUB_NAME>"

# Read stream from Event Hubs
event_hub_config = {
    "eventhubs.connectionString": connectionString
}

df = spark.readStream.format("eventhubs").options(**event_hub_config).load()

# Extract the body of the Event Hub messages
events_df = df.withColumn("body", col("body").cast("string"))

# Convert the body to a number and add the Risk column
parsed_df = events_df.withColumn("number", col("body").cast("integer"))
risk_df = parsed_df.withColumn("Risk", expr("CASE WHEN number > 80 THEN 'High' ELSE 'Low' END"))

# Write the streaming data to console (for testing)
query = risk_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
