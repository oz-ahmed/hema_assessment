# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, split, lit, to_date, expr, date_sub, add_months, 
    current_timestamp, sha2, concat_ws, when, element_at, size, trim, max as spark_max, current_timestamp
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType

# Define a single timestamp for the entire run
run_timestamp = current_timestamp()
high_date = to_date(lit("2020-12-31"))

# Read from Silver staging table
silver_df = spark.read.parquet(silver_location)

# Anchor date for aggregate calculations
latest_date = silver_df.select(spark_max("order_date")).first()[0]

# COMMAND ----------

# Calculate aggregates
customer_agg_df = silver_df.groupBy("customer_id", "customer_name", "segment", "country") \
    .agg(
        count(col("order_id")).alias("total_quantity_of_orders"),
        count(expr(f"CASE WHEN order_date BETWEEN date_sub('{latest_date}', 30) AND '{latest_date}' THEN order_id ELSE NULL END"))
            .alias("qty_orders_last_1m"),
        count(expr(f"CASE WHEN order_date BETWEEN add_months('{latest_date}', -6) AND '{latest_date}' THEN order_id ELSE NULL END"))
            .alias("qty_orders_last_6m"),
        count(expr(f"CASE WHEN order_date BETWEEN add_months('{latest_date}', -12) AND '{latest_date}' THEN order_id ELSE NULL END"))
            .alias("qty_orders_last_12m")
    )

# Add name transformations and select final columns
customer_gold_df = (customer_agg_df
    .withColumn("name_parts", split(col("customer_name"), " "))
    .withColumn("name_parts_size", size(col("name_parts")))
    .withColumn("customer_first_name", 
                expr("CASE WHEN name_parts_size >= 1 THEN name_parts[0] ELSE customer_name END"))
    .withColumn("customer_last_name", 
                expr("""
                    CASE 
                        WHEN name_parts_size >= 2 THEN array_join(slice(name_parts, 2, name_parts_size), ' ')
                        ELSE ''
                    END
                """))
    .select(
        "customer_id", "customer_first_name", "customer_last_name", "customer_segment", "country",
        "qty_orders_last_1m", "qty_orders_last_6m", "qty_orders_last_12m", "total_quantity_of_orders",
    )
)

customer_gold_df = (
    customer_gold_df
    .withColumn("file_path", lit(gold_customer_location))
    .withColumn("execution_datetime", current_timestamp())
)

# Write to Gold Customer table (full refresh, as required)
try:
    (
        customer_gold_df.write
        .format("parquet")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_customer_location)
    )
    logger.info("Successfully wrote to gold customer table")
except Exception as e:
    logger.warning(f"Error writing to gold customer table: {e}")

# COMMAND ----------

# Gold sales schema
gold_sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", DateType(), True),
    StructField("ship_mode", StringType(), True),
    StructField("city", StringType(), True),
    StructField("is_current", BooleanType(), False),
    StructField("start_date", TimestampType(), False),
    StructField("end_date", TimestampType(), False)
])

try:
    gold_sales_existing_df = spark.read.schema(gold_sales_schema).parquet(gold_sales_location)
except AnalysisException:
    # Data doesn't exist
    gold_sales_existing_df = spark.createDataFrame([], schema=gold_sales_schema)

# source columns to check for changes
source_df = silver_df.select(
    "order_id", "order_date", "ship_date", "ship_mode", "city"
).dropDuplicates(["order_id"])

# Create a hash of the columns that can change
source_df = source_df.withColumn(
    "data_hash", sha2(concat_ws("||", "order_id", "ship_date", "ship_mode", "city"), 256)
)

# COMMAND ----------

# Compare *current* records with *existing* records
target_current_df = gold_sales_existing_df.filter(col("is_current") == True) \
    .withColumn(
        "data_hash", sha2(concat_ws("||", "ship_date", "ship_mode", "city"), 256)
    )

# Use aliases to handle column name collisions
source_aliased = source_df.alias("source")
target_aliased = target_current_df.alias("target")

# Join source and target
joined_df = source_aliased.join(
    target_aliased,
    col("source.order_id") == col("target.order_id"),
    "full_outer"
)

# COMMAND ----------

# Case 1: New records (in source, not in target)
new_records_df = (
    joined_df
    .filter(col("target.order_id").isNull())
    .select(
        col("source.order_id").alias("order_id"),
        col("source.order_date").alias("order_date"),
        col("source.ship_date").alias("ship_date"),
        col("source.ship_mode").alias("ship_mode"),
        col("source.city").alias("city"),
        lit(True).alias("is_current"),
        run_timestamp.alias("start_date"),
        high_date.cast("timestamp").alias("end_date")
    )
)

# Case 2: Unchanged records (in both, hash matches)
unchanged_records_df = (
    joined_df
    .filter(
        col("source.order_id").isNotNull() & 
        col("target.order_id").isNotNull() &
        (col("source.data_hash") == col("target.data_hash"))
    )
    .select(
        col("target.order_id").alias("order_id"),
        col("target.order_date").alias("order_date"),
        col("target.ship_date").alias("ship_date"),
        col("target.ship_mode").alias("ship_mode"),
        col("target.city").alias("city"),
        col("target.is_current").alias("is_current"),
        col("target.start_date").alias("start_date"),
        col("target.end_date").alias("end_date")
    )
)

# Case 3: Changed records (in both, hash mismatch)
# 3a. The *new* version of the record (from source)
changed_new_records_df = (
    joined_df
    .filter(
        col("source.order_id").isNotNull() & 
        col("target.order_id").isNotNull() &
        (col("source.data_hash") != col("target.data_hash"))
    )
    .select(
        col("source.order_id").alias("order_id"),
        col("source.order_date").alias("order_date"),
        col("source.ship_date").alias("ship_date"),
        col("source.ship_mode").alias("ship_mode"),
        col("source.city").alias("city"),
        lit(True).alias("is_current"),
        run_timestamp.alias("start_date"),
        high_date.cast("timestamp").alias("end_date")
    )
)

# 3b. The *expired* version of the record (from target)
changed_expired_records_df = (
    joined_df
    .filter(
        col("source.order_id").isNotNull() & 
        col("target.order_id").isNotNull() &
        (col("source.data_hash") != col("target.data_hash"))
    )
    .select(
        col("target.order_id").alias("order_id"),
        col("target.order_date").alias("order_date"),
        col("target.ship_date").alias("ship_date"),
        col("target.ship_mode").alias("ship_mode"),
        col("target.city").alias("city"),
        lit(False).alias("is_current"),
        col("target.start_date").alias("start_date"),
        run_timestamp.alias("end_date")  # Expire it with the current run time
    )
)

# Case 4: Old history (records that were already not current)
old_history_df = gold_sales_existing_df.filter(col("is_current") == False)

# COMMAND ----------

# Combine all parts into the new Gold table
final_sales_df = (
    new_records_df
    .unionByName(unchanged_records_df)
    .unionByName(changed_new_records_df)
    .unionByName(changed_expired_records_df)
    .unionByName(old_history_df)
)

final_sales_df = (
    final_sales_df
    .withColumn("file_path", lit(gold_sales_location))
    .withColumn("execution_datetime", current_timestamp())
)

# Overwrite the entire Parquet table with the new, combined data
try:
    (
        final_sales_df.write
        .format("parquet")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("partition_year", "partition_month", "partition_day")
        .save(gold_sales_location)
    )
    logger.info(f"Success! Updated gold sales location: {gold_sales_location}")
except Exception as e:
    logger.warning(f"Error! Failed writing to gold sales location: {gold_sales_location}: {e}")
    
print(f"Run successful.")
display(final_sales_df)