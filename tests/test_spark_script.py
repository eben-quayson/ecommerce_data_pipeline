import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.compute_kpis import store_category_kpis_in_dynamodb, store_order_kpis_in_dynamodb

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import to_date, col, countDistinct, sum, when    

# Fixture for Spark session
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("Test").getOrCreate()

# Sample data for mocking
@pytest.fixture
def sample_data(spark):
    orders_data = [
        Row(order_id="1", created_at="2024-01-01", user_id="101"),
        Row(order_id="2", created_at="2024-01-02", user_id="102"),
    ]
    items_data = [
        Row(id="1", order_id="1", product_id="p1", sale_price=100.0, status="delivered", created_at="2024-01-01"),
        Row(id="2", order_id="2", product_id="p2", sale_price=200.0, status="returned", created_at="2024-01-02"),
    ]
    products_data = [
        Row(id="p1", category="electronics"),
        Row(id="p2", category="books"),
    ]

    return {
        "orders_df": spark.createDataFrame(orders_data),
        "order_items_df": spark.createDataFrame(items_data),
        "products_df": spark.createDataFrame(products_data),
    }

def test_category_kpis(spark, sample_data):
    items = sample_data["order_items_df"]
    products = sample_data["products_df"]

    items_with_category = items.join(products, items.product_id == products.id, "inner")
    items_with_category = items_with_category.withColumn("order_date", to_date("created_at"))

    category_kpis = items_with_category.groupBy("category", "order_date").agg(
        sum("sale_price").alias("daily_revenue"),
        (sum("sale_price") / countDistinct("order_id")).alias("avg_order_value"),
        (sum(when(col("status") == "returned", 1).otherwise(0)) / countDistinct("order_id")).alias("avg_return_rate")
    )

    results = category_kpis.collect()
    assert len(results) == 2
    assert set([row["category"] for row in results]) == {"electronics", "books"}

def test_order_kpis(spark, sample_data):
    orders = sample_data["orders_df"].withColumn("order_date", to_date("created_at"))
    items = sample_data["order_items_df"]

    joined = orders.join(items, "order_id", "left")

    order_kpis = joined.groupBy("order_date").agg(
        countDistinct("order_id").alias("total_orders"),
        sum("sale_price").alias("total_revenue"),
        countDistinct("id").alias("total_items_sold"),
        (sum(when(col("status") == "returned", 1).otherwise(0)) / countDistinct("order_id")).alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )

    results = order_kpis.collect()
    assert len(results) == 2
    assert all(row["total_orders"] == 1 for row in results)

@patch("scripts.compute_kpis.boto3.resource")
def test_store_category_kpis_in_dynamodb(mock_boto, spark, sample_data):
    from scripts.compute_kpis import store_category_kpis_in_dynamodb

    mock_table = MagicMock()
    mock_boto.return_value.Table.return_value = mock_table

    df = spark.createDataFrame([
        Row(category="books", order_date="2024-01-01", daily_revenue=200.0, avg_order_value=200.0, avg_return_rate=0.5)
    ])
    
    store_category_kpis_in_dynamodb(df)
    mock_table.put_item.assert_called_once()

@patch("scripts.compute_kpis.boto3.resource")
def test_store_order_kpis_in_dynamodb(mock_boto, spark, sample_data):
    from scripts.compute_kpis import store_order_kpis_in_dynamodb

    mock_table = MagicMock()
    mock_boto.return_value.Table.return_value = mock_table

    df = spark.createDataFrame([
        Row(order_date="2024-01-01", total_orders=1, total_revenue=100.0, total_items_sold=1, return_rate=0.0, unique_customers=1)
    ])
    
    store_order_kpis_in_dynamodb(df)
    mock_table.put_item.assert_called_once()

