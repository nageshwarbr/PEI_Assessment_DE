# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
!pip install openpyxl

# COMMAND ----------

# from bronze_ingestion_functions import ingest_csv,ingest_json,ingest_excel

# COMMAND ----------

# MAGIC %run ./bronze_functions

# COMMAND ----------

ingest_csv(spark,"/Volumes/ecommerce_catalog/bronze/raw_file/products/", "products")
ingest_json(spark,"/Volumes/ecommerce_catalog/bronze/raw_file/orders/", "orders")
ingest_excel(spark,"/Volumes/ecommerce_catalog/bronze/raw_file/customer/Customer.xlsx","Worksheet" ,"customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_catalog.bronze.customer

# COMMAND ----------

