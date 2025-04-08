import pytest
import os
import sys
import boto3
from moto import mock_aws # Use mock_aws decorator
from pyspark.sql import SparkSession
from decimal import Decimal
import datetime
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the functions to test (assuming they are in process_kpis.py)
from scripts.compute_kpis import (
    store_category_kpis_in_dynamodb,
    store_order_kpis_in_dynamodb,
    calculate_category_kpis,
    calculate_order_kpis,
    run_kpi_job
)

# --- Fixtures ---
# In your test_compute_kpis.py or conftest.py

@pytest.fixture(scope="session")
def spark_session():
    """Creates a Spark session configured for S3A access (for testing)."""
    hadoop_aws_jar = "org.apache.hadoop:hadoop-aws:3.3.4"
    aws_sdk_jar = "com.amazonaws:aws-java-sdk-bundle:1.12.367"

    builder = SparkSession.builder \
        .appName("pytest-spark-testing") \
        .master("local[2]") \
        .config("spark.sql.decimalOperations.allowPrecisionLoss", "false") \
        .config("spark.jars.packages", f"{hadoop_aws_jar},{aws_sdk_jar}") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    spark = builder.getOrCreate()

    # --- Add this line ---
    print(f"\n\n--- Detected Spark Version: {spark.version} ---\n\n")
    # --- End Add ---

    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def spark_session():
    """Creates a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("pytest-spark-testing") \
        .master("local[2]") \
        .config("spark.sql.decimalOperations.allowPrecisionLoss", "false") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="function") # Use function scope for isolation
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1" # Choose a region

@pytest.fixture(scope="function")
def mock_aws_env(aws_credentials):
    """Sets up mocked S3 and DynamoDB environments."""
    with mock_aws(): # Use mock_aws context manager within fixture
        s3_client = boto3.client("s3")
        dynamodb_res = boto3.resource("dynamodb")

        # --- S3 Setup ---
        bucket_name = "mock-ecom-bucket-gyenyame"
        s3_client.create_bucket(Bucket=bucket_name)

        # Sample Data (as strings for CSV)
        orders_csv = "order_id,user_id,status,created_at\n101,1,completed,2023-01-15 10:00:00\n102,2,completed,2023-01-15 11:30:00\n103,1,cancelled,2023-01-16 09:00:00"
        order_items_csv = "id,order_id,product_id,sale_price,status,created_at\n1,101,1,10.50,shipped,2023-01-15 10:01:00\n2,101,2,25.00,shipped,2023-01-15 10:01:00\n3,102,1,11.00,returned,2023-01-15 11:31:00\n4,103,3,5.00,pending,2023-01-16 09:01:00" # Note: Using item created_at for date extraction in category kpi calc
        products_csv = "id,category,name\n1,Electronics,Laptop\n2,Home,Chair\n3,Electronics,Mouse"

        # Upload to Mock S3
        s3_base_path = f"{bucket_name}/Data"
        s3_client.put_object(Bucket=bucket_name, Key="Data/orders/orders.csv", Body=orders_csv)
        s3_client.put_object(Bucket=bucket_name, Key="Data/order_items/order_items.csv", Body=order_items_csv)
        s3_client.put_object(Bucket=bucket_name, Key="Data/products/products.csv", Body=products_csv)

        # --- DynamoDB Setup ---
        category_table_name = "mock_category_kpis"
        order_table_name = "mock_order_kpis"

        # Create Category KPIs Table
        dynamodb_res.create_table(
            TableName=category_table_name,
            KeySchema=[
                {'AttributeName': 'category', 'KeyType': 'HASH'}, # Partition key
                {'AttributeName': 'order_date', 'KeyType': 'RANGE'}  # Sort key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'category', 'AttributeType': 'S'},
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Create Order KPIs Table
        dynamodb_res.create_table(
            TableName=order_table_name,
            KeySchema=[
                {'AttributeName': 'order_date', 'KeyType': 'HASH'}  # Partition key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Make clients/resources available to tests if needed
        yield {
            "s3_client": s3_client,
            "dynamodb_res": dynamodb_res,
            "bucket_name": bucket_name,
            "s3_base_path": f"s3://{s3_base_path}", # Correct S3 path format
            "category_table_name": category_table_name,
            "order_table_name": order_table_name
        }
        # Teardown happens automatically when exiting 'with mock_aws()'

# --- Test Functions ---

def test_calculate_category_kpis(spark_session):
    """Tests the category KPI calculation logic."""
    # Input DataFrames
    products_data = [(1, "Electronics", "Laptop"), (2, "Home", "Chair"), (3, "Electronics", "Mouse")]
    products_df = spark_session.createDataFrame(products_data, ["id", "category", "name"])

    order_items_data = [
        (1, 101, 1, 10.50, "shipped", "2023-01-15 10:01:00"), # Elec
        (2, 101, 2, 25.00, "shipped", "2023-01-15 10:01:00"), # Home
        (3, 102, 1, 11.00, "returned", "2023-01-15 11:31:00"), # Elec, returned
        (4, 103, 3, 5.00, "pending", "2023-01-16 09:01:00"),  # Elec
    ]
    order_items_df = spark_session.createDataFrame(order_items_data, ["id", "order_id", "product_id", "sale_price", "status", "created_at"])

    # Expected Results (Manual Calculation)
    # Date: 2023-01-15
    #   Electronics: Revenue=10.50+11.00=21.50, Orders=2 (101, 102), AOV=21.50/2=10.75, Returns=1, Rate=1/2=0.5
    #   Home: Revenue=25.00, Orders=1 (101), AOV=25.00/1=25.00, Returns=0, Rate=0/1=0.0
    # Date: 2023-01-16
    #   Electronics: Revenue=5.00, Orders=1 (103), AOV=5.00/1=5.00, Returns=0, Rate=0/1=0.0
    expected_data = [
        ("Electronics", datetime.date(2023, 1, 15), 21.50, 10.75, 0.5),
        ("Home", datetime.date(2023, 1, 15), 25.00, 25.00, 0.0),
        ("Electronics", datetime.date(2023, 1, 16), 5.00, 5.00, 0.0),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["category", "order_date", "daily_revenue", "avg_order_value", "avg_return_rate"])

    # Actual Result
    actual_df = calculate_category_kpis(order_items_df, products_df)

    # Comparison (convert to Pandas for easier comparison, sort to ensure order)
    expected_pd = expected_df.orderBy("category", "order_date").toPandas()
    actual_pd = actual_df.orderBy("category", "order_date").toPandas()

    pd.testing.assert_frame_equal(expected_pd, actual_pd, check_dtype=False, check_like=True, rtol=1e-5)


def test_calculate_order_kpis(spark_session):
    """Tests the order KPI calculation logic."""
    # Input DataFrames
    orders_data = [
        (101, 1, "completed", "2023-01-15 10:00:00"),
        (102, 2, "completed", "2023-01-15 11:30:00"),
        (103, 1, "cancelled", "2023-01-16 09:00:00"), # User 1 again
    ]
    orders_df = spark_session.createDataFrame(orders_data, ["order_id", "user_id", "status", "created_at"])

    order_items_data = [
        (1, 101, 1, 10.50, "shipped"), # Item ID 1
        (2, 101, 2, 25.00, "shipped"), # Item ID 2
        (3, 102, 1, 11.00, "returned"),# Item ID 3
        (4, 103, 3, 5.00, "pending"),  # Item ID 4
    ]
    # Add a dummy created_at if needed by the function, or ensure the join brings it
    order_items_df = spark_session.createDataFrame(order_items_data, ["id", "order_id", "product_id", "sale_price", "status"])

    # Expected Results (Manual Calculation)
    # Date: 2023-01-15
    #   Orders: 2 (101, 102), Revenue=10.50+25.00+11.00=46.50, Items=3 (1,2,3), Returns=1, Rate=1/2=0.5, Customers=2 (1, 2)
    # Date: 2023-01-16
    #   Orders: 1 (103), Revenue=5.00, Items=1 (4), Returns=0, Rate=0/1=0.0, Customers=1 (1)
    expected_data = [
        (datetime.date(2023, 1, 15), 2, 46.50, 3, 0.5, 2),
        (datetime.date(2023, 1, 16), 1, 5.00, 1, 0.0, 1),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["order_date", "total_orders", "total_revenue", "total_items_sold", "return_rate", "unique_customers"])

    # Actual Result
    actual_df = calculate_order_kpis(orders_df, order_items_df)

    # Comparison
    expected_pd = expected_df.orderBy("order_date").toPandas()
    actual_pd = actual_df.orderBy("order_date").toPandas()

    pd.testing.assert_frame_equal(expected_pd, actual_pd, check_dtype=False, check_like=True, rtol=1e-5)


def test_store_category_kpis(spark_session, mock_aws_env):
    """Tests writing category KPIs to mock DynamoDB."""
    dynamodb_res = mock_aws_env["dynamodb_res"]
    table_name = mock_aws_env["category_table_name"]
    table = dynamodb_res.Table(table_name)

    # Sample KPI data to write
    kpi_data = [
        ("Electronics", datetime.date(2023, 1, 15), 21.50, 10.75, 0.5),
        ("Home", datetime.date(2023, 1, 15), 25.00, 25.00, 0.0),
    ]
    kpi_df = spark_session.createDataFrame(kpi_data, ["category", "order_date", "daily_revenue", "avg_order_value", "avg_return_rate"])

    # Call the function
    store_category_kpis_in_dynamodb(kpi_df, table_name)

    # Verify data in mock DynamoDB
    response = table.scan()
    items = response['Items']

    # Sort for consistent comparison
    items.sort(key=lambda x: (x['category'], x['order_date']))

    expected_items = [
        {'category': 'Electronics', 'order_date': '2023-01-15', 'daily_revenue': Decimal('21.50'), 'avg_order_value': Decimal('10.75'), 'avg_return_rate': Decimal('0.5')},
        {'category': 'Home', 'order_date': '2023-01-15', 'daily_revenue': Decimal('25.00'), 'avg_order_value': Decimal('25.00'), 'avg_return_rate': Decimal('0.0')},
    ]

    assert len(items) == 2
    assert items == expected_items


def test_store_order_kpis(spark_session, mock_aws_env):
    """Tests writing order KPIs to mock DynamoDB."""
    dynamodb_res = mock_aws_env["dynamodb_res"]
    table_name = mock_aws_env["order_table_name"]
    table = dynamodb_res.Table(table_name)

    # Sample KPI data to write
    kpi_data = [
        (datetime.date(2023, 1, 15), 2, 46.50, 3, 0.5, 2),
        (datetime.date(2023, 1, 16), 1, 5.00, 1, 0.0, 1),
    ]
    kpi_df = spark_session.createDataFrame(kpi_data, ["order_date", "total_orders", "total_revenue", "total_items_sold", "return_rate", "unique_customers"])

    # Call the function
    store_order_kpis_in_dynamodb(kpi_df, table_name)

    # Verify data in mock DynamoDB
    response = table.scan()
    items = response['Items']

     # Sort for consistent comparison
    items.sort(key=lambda x: x['order_date'])

    expected_items = [
         {'order_date': '2023-01-15', 'total_orders': 2, 'total_revenue': Decimal('46.50'), 'total_items_sold': 3, 'return_rate': Decimal('0.5'), 'unique_customers': 2},
         {'order_date': '2023-01-16', 'total_orders': 1, 'total_revenue': Decimal('5.00'), 'total_items_sold': 1, 'return_rate': Decimal('0.0'), 'unique_customers': 1},
    ]

    assert len(items) == 2
    assert items == expected_items


def test_run_kpi_job_e2e(spark_session, mock_aws_env):
    """Tests the full job reading from mock S3 and writing to mock DynamoDB."""
    s3_path = mock_aws_env["s3_base_path"]
    cat_table_name = mock_aws_env["category_table_name"]
    ord_table_name = mock_aws_env["order_table_name"]
    dynamodb_res = mock_aws_env["dynamodb_res"]

    # Run the entire job
    run_kpi_job(spark_session, s3_path, cat_table_name, ord_table_name)

    # Verify Category Table
    cat_table = dynamodb_res.Table(cat_table_name)
    cat_response = cat_table.scan()
    cat_items = cat_response['Items']
    cat_items.sort(key=lambda x: (x['category'], x['order_date'])) # Sort for predictable order

    expected_cat_items = [
        {'category': 'Electronics', 'order_date': '2023-01-15', 'daily_revenue': Decimal('21.50'), 'avg_order_value': Decimal('10.75'), 'avg_return_rate': Decimal('0.5')},
        {'category': 'Electronics', 'order_date': '2023-01-16', 'daily_revenue': Decimal('5.00'), 'avg_order_value': Decimal('5.00'), 'avg_return_rate': Decimal('0.0')},
        {'category': 'Home', 'order_date': '2023-01-15', 'daily_revenue': Decimal('25.00'), 'avg_order_value': Decimal('25.00'), 'avg_return_rate': Decimal('0.0')},
    ]
    assert len(cat_items) == 3
    # Convert actual items for comparison if needed (e.g., if Decimal types differ slightly)
    # This simple comparison might fail if types aren't exactly Decimal, adjust as needed.
    assert cat_items == expected_cat_items

    # Verify Order Table
    ord_table = dynamodb_res.Table(ord_table_name)
    ord_response = ord_table.scan()
    ord_items = ord_response['Items']
    ord_items.sort(key=lambda x: x['order_date']) # Sort

    expected_ord_items = [
         {'order_date': '2023-01-15', 'total_orders': 2, 'total_revenue': Decimal('46.50'), 'total_items_sold': 3, 'return_rate': Decimal('0.5'), 'unique_customers': 2},
         {'order_date': '2023-01-16', 'total_orders': 1, 'total_revenue': Decimal('5.00'), 'total_items_sold': 1, 'return_rate': Decimal('0.0'), 'unique_customers': 1},
    ]
    assert len(ord_items) == 2
    assert ord_items == expected_ord_items