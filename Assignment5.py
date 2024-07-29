# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.streaming import DataStreamWriter

# Initialize Spark Session
spark = SparkSession.builder.appName("EventHubStream").getOrCreate()

# Event Hub connection configuration for input
input_connection_string = "Endpoint=sb://<INPUT_NAMESPACE_NAME>.servicebus.windows.net/;SharedAccessKeyName=<SHARED_ACCESS_KEY_NAME>;SharedAccessKey=<SHARED_ACCESS_KEY>;EntityPath=<INPUT_EVENT_HUB_NAME>"

# Event Hub connection configuration for output
output_connection_string = "Endpoint=sb://<OUTPUT_NAMESPACE_NAME>.servicebus.windows.net/;SharedAccessKeyName=<SHARED_ACCESS_KEY_NAME>;SharedAccessKey=<SHARED_ACCESS_KEY>;EntityPath=<OUTPUT_EVENT_HUB_NAME>"

# Read stream from input Event Hub
input_event_hub_config = {
    "eventhubs.connectionString": input_connection_string
}

df = spark.readStream.format("eventhubs").options(**input_event_hub_config).load()

# Extract the body of the Event Hub messages and convert to integer
events_df = df.withColumn("body", col("body").cast("string"))
parsed_df = events_df.withColumn("number", col("body").cast("integer"))

# Add the Risk column
risk_df = parsed_df.withColumn("Risk", expr("CASE WHEN number > 80 THEN 'High' ELSE 'Low' END"))

# Prepare the output stream in a format suitable for Event Hubs (Avro format)
output_df = risk_df.selectExpr("to_avro(struct(*)) AS body")

# Write stream to output Event Hub
output_event_hub_config = {
    "eventhubs.connectionString": output_connection_string
}

query = output_df.writeStream \
    .format("eventhubs") \
    .options(**output_event_hub_config) \
    .option("checkpointLocation", "/mnt/eventhubs-checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
