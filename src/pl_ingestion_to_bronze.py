# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, col

# Read the raw CSV from S3
try:
    raw_df = (
        spark.read.csv(
        SOURCE_S3_PATH, header=True, inferSchema=True
        )
    )
    logger.info(f"Successfully read {raw_df.count()} rows from {SOURCE_S3_PATH}.")
except Exception as e:
    logger.error(f"Error reading source data: {str(e)}")
    raise
# Add metadata columns
raw_df = (raw_df
    .withColumn("File_path", col("_metadata.file_path"))
    .withColumn("execution_datetime", current_timestamp())
)

# display(raw_df_with_metadata)

# COMMAND ----------

# Using 'overwrite' and 'overwriteSchema' to handle schema evolution 
# This is the Parquet equivalent of 'mergeSchema' on write.
try:
    (
        raw_df.write
        .format("parquet")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_location)
    )
    logger.info(f"Successfully wrote {raw_df.count()} rows to {bronze_location}.")
except Exception as e:
    logger.error(f"Error writing to bronze: {str(e)}")
    raise
