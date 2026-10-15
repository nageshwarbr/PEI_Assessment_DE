import pytest
import os
from unittest.mock import MagicMock,call
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

# @pytest.fixture(scope="session")
# def spark():
#     return SparkSession.builder \
#         .appName('unit-tests') \
#         .getOrCreate()

@pytest.fixture
def products_df():
    return spark.createDataFrame(
        [
            ('BVje', 'KpRt', 'XmQa', None, 'NzYu', 1.000),
(None, 'TrVa', 'JmPe', 'WsGi', 'HbCx', 2.34657),
('QwEr', None, None, None, None, -3.847933)
        ],
        [
            'Product ID',
            'Category',
            'Sub-Category',
            'Product Name',
            'State',
            'Price per product'
        ]
    )

@pytest.fixture
def customer_phn_df():
    return spark.createDataFrame(
        [
            ('1', 'John*$&', 'john@.com', '1234*%&(  567890  ', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
            ('1', 'John*$&', 'john@.com', '*%&(  ', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
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

@pytest.fixture
def customer_df():
    return spark.createDataFrame(
        [
            ('1', 'John*$&', 'john@.com', '1234*%&(  567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
            ('1', 'John*$&', 'john@.com', '1234*%&(  567890', '123 Main St', 'Premium', None, 'New YoRk', 'NY', '10001', 'Northeast'),
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

@pytest.fixture
def orders_df():
    return spark.createDataFrame(
            [('JK-15370',	0.3	,'21/8/2016','CA-2016-122581',573.743,'FUR-CH-10002961',	63.69,  7,	1,	'25/8/2016','Standard Class'),
('BD-11320',	0.0	,'23/9/2017','CA-2017-117485',291.943,'TEC-AC-10004659',	102.195, 4	,2,	'29/9/2017',	'Standard Class'),
('LB-16795',	0.754	,'6/10/2016','US-2016-157490',17.0	,'OFF-BI-10002824',	-14.92, 4	,3,	'7/10/2016',	'First Class'   ),
('LB-16795',	0.7	,'6/10/2016','US-2016-157490',17.0	,'OFF-BI-10002824',	-14.92, 4	,None,	'7/10/2016',	'First Class'   ),
('LB-16795',	0.754	,'6/10/2016','US-2016-157490',17.0	,'OFF-BI-10002824',	-14.92, 4	,3,	'7/10/2016',	'First Class'   ),
('KB-16315',	0.253	,'2/7/2015' ,'CA-2015-111703',15.55	,'OFF-PA-10003349',	5.64,   3,	4,	'9/7/2015'	,   'Standard Class'),
('DO-13435',	0.2323	,'3/10/2014','CA-2014-108903',142.49,'TEC-AC-10003023',	-3.7788	,   3,   5,	'3/10/2014',	'Same Day'),
('DO-13435',	0.2323	,'10/10/2014','CA-2014-108903',142.49,'TEC-AC-2432242',	-3.778	,   3,   6,	'3/10/2014',	'Same Day')],
            [
                'Customer ID',
                'Discount',
                'Order Date',
                'Order ID',
                'Price',
                'Product ID',
                'Profit',
                'Quantity',
                'Row ID',
                'Ship Date',
                'Ship Mode'
            ]
        )
@pytest.fixture
def orders_df_validate():
    orders_df_validate_schema = StructType([
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
    orders_df_validate_df_1 = spark.createDataFrame(
            [('JK-15370', 0.3,   '2016-08-21', 'CA-2016-122581', 573.74, 'FUR-CH-10002961', 63.69,   7, 1,    '2016-08-25', 'Standard Class'),
    ('BD-11320', 0.0,   '2017-09-23', 'CA-2017-117485', 291.94, 'TEC-AC-10004659', 102.20, 4, 2,    '2017-09-29', 'Standard Class'),
    ('LB-16795', 0.75, '2016-10-06', 'US-2016-157490', 17.0,    'OFF-BI-10002824', -14.92,  4, 3,    '2016-10-07', 'First Class'),
    ('KB-16315', 0.25, '2015-07-02', 'CA-2015-111703', 15.55,   'OFF-PA-10003349', 5.64,    3, 4,    '2015-07-09', 'Standard Class'),
    ('DO-13435', 0.23,'2014-10-03', 'CA-2014-108903', 142.49,  'TEC-AC-10003023', -3.78,  3, 5,    '2014-10-03', 'Same Day'),
    ('DO-13435', 0.23,'2014-10-10', 'CA-2014-108903', 142.49,  'TEC-AC-10003023', -3.78,  3, 5,    '2014-10-03', 'Same Day')],
            orders_df_validate_schema
        )
    return orders_df_validate_df_1.withColumn('Order Date', F.to_date(F.col('Order Date'), 'yyyy-MM-dd')).withColumn('Ship Date', F.to_date(F.col('Ship Date'), 'yyyy-MM-dd'))