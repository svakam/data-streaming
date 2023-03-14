# Databricks notebook source
# DBTITLE 1,Python: Auto Loader to Delta Lake
##### Config: DB Auto Loader to ingest data to Delta Lake #####
# -------------------------------------------------------- #


# Import functions
from pyspark.sql.functions import input_file_name, current_timestamp

# Define variables used in code below: 
# File path, username, table name, checkpoint
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_test"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_test"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .toTable(table_name))

# PMP: What are pros/cons of using JSON vs. CSV? 
# JSON: streaming in batches (e.g. every 50000), won't get all data at once
# CSV: all data is present, may be harder to divide workload
