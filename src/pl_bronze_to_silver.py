# Databricks notebook source
# MAGIC %run ./config


from pyspark.sql.functions import col, to_date, year, month, dayofmonth, current_timestamp, lit
import re

# Helper function to convert column names to snake_case 
def to_snake_case(s):
    if s is None: return ""
    s = re.sub(r'(?<!^)(?=[A-Z])', '', s).lower()
    return s.replace(' ', '_')

# Read from Bronze.
bronze_df = spark.read.parquet(bronze_location)

# Rename all columns to snake_case
try:
    new_column_names = [to_snake_case(c) for c in bronze_df.columns]
    silver_df = bronze_df.toDF(*new_column_names)
    logger.info(f"Renamed columns to snake_case: {new_column_names}")
except Exception as e:
    logger.error(f"Could not parse the columns to snake_case [{bronze_df.columns}]: {e}")
    raise


# Apply transformations and add partitioning columns 
try:
    silver_df = (
        silver_df
        .withColumn("order_date", to_date(col("order_date"), "dd/MM/yyyy"))
        .withColumn("ship_date", to_date(col("ship_date"), "dd/MM/yyyy"))
        .withColumn("file_path", lit(silver_location))
        .withColumn("execution_datetime", current_timestamp())
        .withColumn("partition_year", year(col("order_date")))
        .withColumn("partition_month", month(col("order_date")))
        .withColumn("partition_day", dayofmonth(col("order_date")))
    )
except Exception as e:
    logger.warning(f"Error applying partitions to Silver: {e}")
    raise


# Write to Silver
try:
    (
        silver_df.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("partition_year", "partition_month", "partition_day")
        .option("overwriteSchema", "true")
        .save(silver_location)
    )
    logger.info(f"Wrote to Silver: {silver_location}")
except Exception as e:
    logger.warning(f"Error writing to Silver {silver_location}: {e}")
    