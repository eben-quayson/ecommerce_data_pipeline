import pytest
from unittest.mock import MagicMock, patch
import boto3
from moto import mock_dynamodb
from pyspark.sql import SparkSession
from pyspark.sql import Row
from compute_kpis import store_category_kpis_in_dynamodb, store_order_kpis_in_dynamodb


@pytest.fixture(scope="module")
def spark_session():
    """Fixture to create a Spark session for testing."""
    return SparkSession.builder.master("local").appName("Test").getOrCreate()


@mock_dynamodb
def test_store_category_kpis_in_dynamodb(spark_session):
    # Mock DynamoDB
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table_name = "category_kpis_table"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "category", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "category", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    # Create a mock DataFrame
    data = [
        Row(category="Electronics", order_date="2025-04-08", daily_revenue=1000.0, avg_order_value=200.0, avg_return_rate=0.05),
        Row(category="Books", order_date="2025-04-08", daily_revenue=500.0, avg_order_value=50.0, avg_return_rate=0.02),
    ]
    category_kpis = spark_session.createDataFrame(data)

    # Call the function
    store_category_kpis_in_dynamodb(category_kpis)

    # Verify data in DynamoDB
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response["Items"]

    assert len(items) == 2
    assert items[0]["category"] == "Electronics"
    assert items[1]["category"] == "Books"


@mock_dynamodb
def test_store_order_kpis_in_dynamodb(spark_session):
    # Mock DynamoDB
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table_name = "order_kpis_table"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "order_date", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "order_date", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    # Create a mock DataFrame
    data = [
        Row(order_date="2025-04-08", total_orders=10, total_revenue=2000.0, total_items_sold=50, return_rate=0.1, unique_customers=8),
        Row(order_date="2025-04-09", total_orders=5, total_revenue=1000.0, total_items_sold=20, return_rate=0.05, unique_customers=4),
    ]
    order_kpis = spark_session.createDataFrame(data)

    # Call the function
    store_order_kpis_in_dynamodb(order_kpis)

    # Verify data in DynamoDB
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response["Items"]

    assert len(items) == 2
    assert items[0]["order_date"] == "2025-04-08"
    assert items[1]["order_date"] == "2025-04-09"