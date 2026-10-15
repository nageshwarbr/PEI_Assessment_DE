# !pip install openpyxl

import pandas as pd

def ingest_csv(spark,filepath, table_name):
    df = spark.read.format("csv").option("header", "true").option("quote", "\"").\
        option("escape", "\"")\
        .option("treatEmptyValuesAsNulls", "true").option("inferSchema", "true").load(filepath)
    df.write.mode("overwrite").option("delta.columnMapping.mode","name").saveAsTable(f"ecommerce_catalog.bronze.{table_name}")

def ingest_json(spark,filepath, table_name):
    df = spark.read.format("json").option("multiLine", "true").load(filepath)
    df.write.mode("overwrite").option("delta.columnMapping.mode","name").saveAsTable(f"ecommerce_catalog.bronze.{table_name}")


def ingest_excel(spark,filepath,sheet,table_name):
    df = pd.read_excel(filepath,sheet_name=sheet,header=0)
    df = df.astype(str)
    df = spark.createDataFrame(df)
    df.write.mode("overwrite").option("delta.columnMapping.mode","name").saveAsTable(f"ecommerce_catalog.bronze.{table_name}")



