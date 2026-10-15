import pytest
import os
from unittest.mock import patch,MagicMock,call
import pandas as pd
from bronze_ingestion_functions import ingest_csv,ingest_json,ingest_excel

def test_ingest_csv():
    spark = MagicMock()
    df = MagicMock()

    spark.read.format.return_value = spark.read
    spark.read.option.return_value = spark.read
    spark.read.load.return_value = df

    df.write.mode.return_value = df.write
    df.write.option.return_value = df.write
    ingest_csv(spark, "/path/", "products")

    spark.read.format.assert_called_once_with("csv")

    expected_options = [
        call("header", "true"),
        call("quote", '"'),
        call("escape", '"'),
        call("treatEmptyValuesAsNulls", "true"),
        call("inferSchema", "true"),
    ]

    assert spark.read.option.call_args_list == expected_options
    spark.read.load.assert_called_once_with("/path/")

    # Assert: verify the write configuration
    df.write.mode.assert_called_once_with("overwrite")

    df.write.option.assert_called_once_with(
        "delta.columnMapping.mode", "name"
    )

    df.write.saveAsTable.assert_called_once_with(
        "ecommerce_catalog.bronze.products"
    )

@patch('pandas.read_excel')
def test_ingest_excel( mock_read_excel):
    sample_pd_df = pd.DataFrame({
        'Customer ID': ['JK-15370', 'BD-11320'],
        'Discount': [0.3, 0.0]
    })
    mock_read_excel.return_value = sample_pd_df
    spark = MagicMock()
    spark_df = MagicMock()
    spark.createDataFrame.return_value = spark_df
    spark_df.write.mode.return_value = spark_df.write
    spark_df.write.option.return_value = spark_df.write

    test_path = "path/to/orders.xlsx"
    test_sheet = "Sheet1"
    test_table = "orders_bronze"
    ingest_excel(spark, test_path, test_sheet, test_table)
    mock_read_excel.assert_called_once_with(test_path, sheet_name=test_sheet, header=0)
    spark.createDataFrame.assert_called_once()
    spark_df.write.mode.assert_called_once_with("overwrite")
    spark_df.write.option.assert_called_once_with("delta.columnMapping.mode", "name")
    spark_df.write.saveAsTable.assert_called_once_with(f"ecommerce_catalog.bronze.{test_table}")

def test_ingest_json():
    spark = MagicMock()
    df = MagicMock()

    spark.read.format.return_value = spark.read
    spark.read.option.return_value = spark.read
    spark.read.load.return_value = df

    df.write.mode.return_value = df.write
    df.write.option.return_value = df.write
    ingest_json(spark, "/path/", "products")

    spark.read.format.assert_called_once_with("json")

    expected_options = [
        call("multiLine", "true")
    ]

    assert spark.read.option.call_args_list == expected_options
    spark.read.load.assert_called_once_with("/path/")

    # Assert: verify the write configuration
    df.write.mode.assert_called_once_with("overwrite")

    df.write.option.assert_called_once_with(
        "delta.columnMapping.mode", "name"
    )

    df.write.saveAsTable.assert_called_once_with(
        "ecommerce_catalog.bronze.products"
    )


def test_ingest_csv_load_failure():
    spark = MagicMock()

    spark.read.format.return_value = spark.read
    spark.read.option.return_value = spark.read
    spark.read.load.side_effect = Exception("File not found")
    with pytest.raises(Exception, match="File not found"):
        ingest_csv(spark, "/invalid/path.csv", "products")

    spark.read.load.assert_called_once_with("/invalid/path.csv")
    
def test_ingest_json_failure():
    spark = MagicMock()
    df = MagicMock()
    spark.read.format.return_value = spark.read
    spark.read.option.side_effect = Exception("File not found")
    with pytest.raises(Exception, match="File not found"):
        ingest_json(spark, "/invalid/path.json", "products")


    