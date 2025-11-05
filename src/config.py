# Databricks notebook source
SOURCE_S3_PATH = "s3://hema-catalog-data/source/*.csv"
bronze_location = "s3://hema-catalog-data/bronze/"
silver_location = "s3://hema-catalog-data/silver/"
gold_customer_location = "s3://hema-catalog-data/gold/customer"
gold_sales_location = "s3://hema-catalog-data/gold/sales"

# COMMAND ----------

import logging
import sys

class PipelineLogger:
    """Custom logger for data pipeline with consistent formatting"""
    
    def __init__(self, name: str, log_level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        
        # Clear existing handlers
        self.logger.handlers = []
        
        # Console handler with formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        
        # Custom format with timestamp, level, and message
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def info(self, message: str):
        self.logger.info(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def error(self, message: str):
        self.logger.error(message)
    
    def debug(self, message: str):
        self.logger.debug(message)
    
    def log_section(self, section_name: str):
        """Log section header for better readability"""
        separator = "=" * 80
        self.logger.info(separator)
        self.logger.info(f"  {section_name}")
        self.logger.info(separator)

# Initialize logger
logger = PipelineLogger("bronze_ingestion_pipeline")
logger.log_section("PIPELINE INITIALIZATION")