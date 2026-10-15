import pytest
import os
from unittest.mock import MagicMock,call
from silver_transformations import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

def test_clean_product_datatypes(products_df):
    expected_df = spark.createDataFrame([
        ('BVje', 'KpRt', 'XmQa', None, 'NzYu', 1.000),
    (None, 'TrVa', 'JmPe', 'WsGi', 'HbCx', 2.347),
    ('QwEr', None, None, None, None, -3.848)
    ], ['Product ID', 'Category', 'Sub-Category', 'Product Name', 'State', 'Price per product'])
    df_result = clean_product_datatypes(products_df)
    assert df_result.schema['Product ID'].dataType == StringType()
    assert df_result.schema['Category'].dataType == StringType()
    assert df_result.schema['Sub-Category'].dataType == StringType()
    assert df_result.schema['Product Name'].dataType == StringType()
    assert df_result.schema['State'].dataType == StringType()
    assert df_result.schema['Price per product'].dataType == DoubleType()
    assert df_result.count() == 3
    assert df_result.collect() == expected_df.collect()

def test_clean_product_names(products_df):
    expected_df = spark.createDataFrame([
        ('BVje', 'Kprt', 'Xmqa', 'Unknown', 'Nzyu', 1.000)
    ], ['Product ID', 'Category', 'Sub-Category', 'Product Name', 'State', 'Price per product'])

    df_result = clean_products_names(products_df)
    assert df_result.schema['Product ID'].dataType == StringType()
    assert df_result.schema['Category'].dataType == StringType()
    assert df_result.schema['Sub-Category'].dataType == StringType()
    assert df_result.schema['Product Name'].dataType == StringType()
    assert df_result.schema['State'].dataType == StringType()
    assert df_result.schema['Price per product'].dataType == DoubleType()
    assert df_result.count() == 1
    assert df_result.collect() == expected_df.collect()

def test_clean_customer_datatypes(customer_df):
    df_result = clean_customer_datatypes(customer_df)
    expected_df = spark.createDataFrame([
            ('1', 'John*$&', 'john@.com', '1234*%&(  567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', 10001, 'Northeast'),
            ('1', 'John*$&', 'john@.com', '1234*%&(  567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', 10001, 'Northeast'),
            ('2', 'Jane  hjR  **#', 'jane@mail.com', '9876543210', '456 Elm St', 'Basic', 'Canada', None, 'ON', 94102, 'Northeast'),
            ('3', 'Bob', 'bobxxxhmail.com', '5555555555', '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, 90001, 'northEast'),
            ('4', 'Bob', None, '5555555555', '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, 90001, 'northEast')
        ],
        [
            'Customer ID',
            'Customer Name',
            'Email',
            'Phone',
            'address',
            'Segment',
            'Country',
            'City',
            'State',
            'Postal Code',
            'Region'
        ]
    )
    assert df_result.schema['Customer ID'].dataType == StringType()
    assert df_result.schema['Customer Name'].dataType == StringType()
    assert df_result.schema['Email'].dataType == StringType()
    assert df_result.schema['Phone'].dataType == StringType()
    assert df_result.schema['address'].dataType == StringType()
    assert df_result.schema['Segment'].dataType == StringType()
    assert df_result.schema['Country'].dataType == StringType()
    assert df_result.schema['City'].dataType == StringType()
    assert df_result.schema['State'].dataType == StringType()
    assert df_result.schema['Postal Code'].dataType == IntegerType()
    assert df_result.schema['Region'].dataType == StringType()
    assert df_result.count()== 5
    assert df_result.collect() == expected_df.collect()

def test_clean_customer_names(customer_df):
    df_result = clean_customer_names(customer_df)
    expected_df = spark.createDataFrame([
           ('1', 'John', 'john@.com', '1234*%&(  567890', '123 Main St', 'Premium', 'Unknown', 'New York', 'Ny', '10001', 'Northeast'),
            ('2', 'Jane Hjr', 'jane@mail.com', '9876543210', '456 Elm St', 'Basic', 'Canada', 'Unknown', 'On', '94102', 'Northeast'),
            ('3', 'Bob', 'bobxxxhmail.com', '5555555555', '789 Oak St', 'Premium', 'Usa', 'Los Angeles', 'Unknown', '90001', 'Northeast'),
            ('4', 'Bob', None, '5555555555', '789 Oak St', 'Premium', 'Usa', 'Los Angeles', 'Unknown', '90001', 'Northeast')
        ],
        [
            'Customer ID',
            'Customer Name',
            'Email',
            'Phone',
            'address',
            'Segment',
            'Country',
            'City',
            'State',
            'Postal Code',
            'Region'
        ]
    )
    assert df_result.schema['Customer ID'].dataType == StringType()
    assert df_result.schema['Customer Name'].dataType == StringType()
    assert df_result.schema['Email'].dataType == StringType()
    assert df_result.schema['Phone'].dataType == StringType()
    assert df_result.schema['address'].dataType == StringType()
    assert df_result.schema['Segment'].dataType == StringType()
    assert df_result.schema['Country'].dataType == StringType()
    assert df_result.schema['City'].dataType == StringType()
    assert df_result.schema['State'].dataType == StringType()
    assert df_result.schema['Postal Code'].dataType == StringType()
    assert df_result.schema['Region'].dataType == StringType()
    assert df_result.count()== 4
    assert df_result.collect() == expected_df.collect()

def test_clean_customer_email(customer_df):
    expected_df = spark.createDataFrame(
        [
            ('1', 'John*$&', 'john@.com', '1234*%&(  567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast',False),
            ('1', 'John*$&', 'john@.com', '1234*%&(  567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast',False),
            ('2', 'Jane  hjR  **#', 'jane@mail.com', '9876543210', '456 Elm St', 'Basic', 'Canada', None, 'ON', '94102', 'Northeast',True),
            ('3', 'Bob', 'bobxxxhmail.com', '5555555555', '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, '90001', 'northEast',False),
            ('4', 'Bob', 'Unknown', '5555555555', '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, '90001', 'northEast',False)
        ],
        [
            'Customer ID',
            'Customer Name',
            'Email',
            'Phone',
            'address',
            'Segment',
            'Country',
            'City',
            'State',
            'Postal Code',
            'Region'
        ]
    )
    df_result = clean_customer_email(customer_df)
    assert df_result.schema['Customer ID'].dataType == StringType()
    assert df_result.schema['Customer Name'].dataType == StringType()
    assert df_result.schema['Email'].dataType == StringType()
    assert df_result.schema['Phone'].dataType == StringType()
    assert df_result.schema['address'].dataType == StringType()
    assert df_result.schema['Segment'].dataType == StringType()
    assert df_result.schema['Country'].dataType == StringType()
    assert df_result.schema['City'].dataType == StringType()
    assert df_result.schema['State'].dataType == StringType()
    assert df_result.schema['Postal Code'].dataType == StringType()
    assert df_result.schema['Region'].dataType == StringType()
    assert df_result.schema['is_valid_email'].dataType == BooleanType()
    assert df_result.count()== 5
    assert df_result.collect() == expected_df.collect()

def test_clean_customer_phone(customer_df,customer_phn_df):
    expected_df_1 = spark.createDataFrame(
        [
            ('1', 'John*$&', 'john@.com', '1234567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
            ('1', 'John*$&', 'john@.com', '1234567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
            ('2', 'Jane  hjR  **#', 'jane@mail.com', '9876543210', '456 Elm St', 'Basic', 'Canada', None, 'ON', '94102', 'Northeast'),
            ('3', 'Bob', 'bobxxxhmail.com', '5555555555', '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, '90001', 'northEast'),
            ('4', 'Bob', None, '5555555555', '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, '90001', 'northEast')
        ],
        [
            'Customer ID',
            'Customer Name',
            'Email',
            'Phone',
            'address',
            'Segment',
            'Country',
            'City',
            'State',
            'Postal Code',
            'Region'
        ]
    )
    df_result_1 = clean_customer_phone(customer_df)
    assert df_result_1.schema['Customer ID'].dataType == StringType()
    assert df_result_1.schema['Customer Name'].dataType == StringType()
    assert df_result_1.schema['Email'].dataType == StringType()
    assert df_result_1.schema['Phone'].dataType == StringType()
    assert df_result_1.schema['address'].dataType == StringType()
    assert df_result_1.schema['Segment'].dataType == StringType()
    assert df_result_1.schema['Country'].dataType == StringType()
    assert df_result_1.schema['City'].dataType == StringType()
    assert df_result_1.schema['State'].dataType == StringType()
    assert df_result_1.schema['Postal Code'].dataType == StringType()
    assert df_result_1.schema['Region'].dataType == StringType()
    assert df_result_1.count()== 5
    assert df_result_1.collect() == expected_df_1.collect()

    expected_df_2 = spark.createDataFrame(
        [
            ('1', 'John*$&', 'john@.com', '1234567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
            ('1', 'John*$&', 'john@.com', None, '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
            ('2', 'Jane  hjR  **#', 'jane@mail.com', '9876543210', '456 Elm St', 'Basic', 'Canada', None, 'ON', '94102', 'Northeast'),
            ('3', 'Bob', 'bobxxxhmail.com', None, '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, '90001', 'northEast'),
            ('4', 'Bob', None, '5555555555', '789 Oak St', 'Premium', 'USA', 'Los Angeles', None, '90001', 'northEast')
        ],
        [
            'Customer ID',
            'Customer Name',
            'Email',
            'Phone',
            'address',
            'Segment',
            'Country',
            'City',
            'State',
            'Postal Code',
            'Region'
        ]
    )
    df_result_2 = clean_customer_phone(customer_phn_df)
    assert df_result_2.schema['Customer ID'].dataType == StringType()
    assert df_result_2.schema['Customer Name'].dataType == StringType()
    assert df_result_2.schema['Email'].dataType == StringType()
    assert df_result_2.schema['Phone'].dataType == StringType()
    assert df_result_2.schema['address'].dataType == StringType()
    assert df_result_2.schema['Segment'].dataType == StringType()
    assert df_result_2.schema['Country'].dataType == StringType()
    assert df_result_2.schema['City'].dataType == StringType()
    assert df_result_2.schema['State'].dataType == StringType()
    assert df_result_2.schema['Postal Code'].dataType == StringType()
    assert df_result_2.schema['Region'].dataType == StringType()
    assert df_result_2.count()== 5
    assert df_result_2.collect() == expected_df_2.collect()

def test_clean_orders_dates(orders_df):
    df_result = clean_orders_dates(orders_df)
    expected_schema = StructType([
    StructField('Customer ID', StringType(), True),
    StructField('Discount', DoubleType(), True),
    StructField('Order Date', StringType(), True),
    StructField('Order ID', StringType(), True),
    StructField('Price', DoubleType(), True),
    StructField('Product ID', StringType(), True),
    StructField('Profit', DoubleType(), True),
    StructField('Quantity', IntegerType(), True),
    StructField('Row ID', IntegerType(), True),
    StructField('Ship Date', StringType(), True),
    StructField('Ship Mode', StringType(), True)
])
    expected_df_1 = spark.createDataFrame(
            [('JK-15370', 0.3,   '2016-08-21', 'CA-2016-122581', 573.74, 'FUR-CH-10002961', 63.69,   7, 1,    '2016-08-25', 'Standard Class'),
    ('BD-11320', 0.0,   '2017-09-23', 'CA-2017-117485', 291.94, 'TEC-AC-10004659', 102.20, 4, 2,    '2017-09-29', 'Standard Class'),
    ('LB-16795', 0.75, '2016-10-06', 'US-2016-157490', 17.0,    'OFF-BI-10002824', -14.92,  4, 3,    '2016-10-07', 'First Class'),
    ('KB-16315', 0.25, '2015-07-02', 'CA-2015-111703', 15.55,   'OFF-PA-10003349', 5.64,    3, 4,    '2015-07-09', 'Standard Class'),
    ('DO-13435', 0.23,'2014-10-03', 'CA-2014-108903', 142.49,  'TEC-AC-10003023', -3.78,  3, 5,    '2014-10-03', 'Same Day'),
    ('DO-13435', 0.23,'2014-10-10', 'CA-2014-108903', 142.49,  'TEC-AC-2432242',  -3.78,   3, 6,    '2014-10-03', 'Same Day')],
            expected_schema
        )
    expected_df_2 = expected_df_1.withColumn('Order Date', F.to_date(F.col('Order Date'), 'yyyy-MM-dd')).withColumn('Ship Date', F.to_date(F.col('Ship Date'), 'yyyy-MM-dd'))
    assert df_result.schema['Order Date'].dataType == DateType()
    assert df_result.schema['Ship Date'].dataType == DateType()
    assert df_result.count() == 6
    assert df_result.collect() == expected_df_2.collect()

def test_clean_orders_validate(orders_df_validate):
    df_result = clean_orders_validate(orders_df_validate)
    expected_schema = StructType([
    StructField('Customer ID', StringType(), True),
    StructField('Discount', DoubleType(), True),
    StructField('Order Date', StringType(), True),
    StructField('Order ID', StringType(), True),
    StructField('Price', DoubleType(), True),
    StructField('Product ID', StringType(), True),
    StructField('Profit', DoubleType(), True),
    StructField('Quantity', IntegerType(), True),
    StructField('Row ID', IntegerType(), True),
    StructField('Ship Date', StringType(), True),
    StructField('Ship Mode', StringType(), True)])
    expected_df_1 = spark.createDataFrame(
            [('JK-15370', 0.3,   '2016-08-21', 'CA-2016-122581', 573.74, 'FUR-CH-10002961', 63.69,   7, 1,    '2016-08-25', 'Standard Class'),
    ('BD-11320', 0.0,   '2017-09-23', 'CA-2017-117485', 291.94, 'TEC-AC-10004659', 102.20, 4, 2,    '2017-09-29', 'Standard Class'),
    ('LB-16795', 0.75, '2016-10-06', 'US-2016-157490', 17.0,    'OFF-BI-10002824', -14.92,  4, 3,    '2016-10-07', 'First Class'),
    ('KB-16315', 0.25, '2015-07-02', 'CA-2015-111703', 15.55,   'OFF-PA-10003349', 5.64,    3, 4,    '2015-07-09', 'Standard Class'),
    ('DO-13435', 0.23,'2014-10-03', 'CA-2014-108903', 142.49,  'TEC-AC-10003023', -3.78,  3, 5,    '2014-10-03', 'Same Day')],
            expected_schema
        )
    expected_df_2 = expected_df_1.withColumn('Order Date', F.to_date(F.col('Order Date'), 'yyyy-MM-dd')).withColumn('Ship Date', F.to_date(F.col('Ship Date'), 'yyyy-MM-dd'))
    df_result.collect() == expected_df_2.collect()
    df_result.schema == expected_schema

def test_enriched_order_customer_product():
    orders = spark.createDataFrame(
        [(1, "C1", "P1", 100.0)],
        ["Row ID", "Customer ID", "Product ID", "Sales"],
    )
    customers = spark.createDataFrame(
        [("C1", "Alice", "India")],
        ["Customer ID", "Customer Name", "Country"],
    )
    products = spark.createDataFrame(
        [("P1", "Furniture", "Chairs")],
        ["Product ID", "Category", "Sub-Category"],
    )

    result = enriched_order_customer_product(orders, customers, products)

    assert set(result.columns) == {
        "Sales", "Customer Name", "Customer Country",
        "Product Category", "Product Sub-Category",
    }
    row = result.collect()[0]
    assert row["Customer Country"] == "India"
    assert row["Product Sub-Category"] == "Chairs"