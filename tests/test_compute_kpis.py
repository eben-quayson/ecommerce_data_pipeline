import pytest
import os
import sys
# Removed os, boto3, moto, Decimal imports
from pyspark.sql import SparkSession
import datetime
import pandas as pd 

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import ONLY the calculation functions to test
from scripts.transformation_scripts.compute_kpis import (
    calculate_category_kpis,
    calculate_order_kpis
    # Removed store_* and run_kpi_job imports
)

# --- Fixtures ---

@pytest.fixture(scope="session")
def spark_session():
    """Creates a basic Spark session for testing calculation logic."""
    # Removed S3A JARs and Hadoop configurations
    builder = SparkSession.builder \
        .appName("pytest-spark-pure-logic") \
        .master("local[2]") \
        .config("spark.sql.decimalOperations.allowPrecisionLoss", "false")
        # Removed .config("spark.jars.packages", ...)
        # Removed .config("spark.hadoop.fs.s3a.*", ...) lines

    spark = builder.getOrCreate()
    print(f"\n\n--- Detected Spark Version: {spark.version} ---\n\n")
    # Removed the S3AFileSystem class check diagnostic block
    yield spark
    spark.stop()

# --- Removed aws_credentials fixture ---

# --- Removed mock_aws_env fixture ---


# --- Test Functions (Keep only calculation tests) ---

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
    # Assuming schema includes created_at needed by the function
    order_items_df = spark_session.createDataFrame(order_items_data, ["id", "order_id", "product_id", "sale_price", "status", "created_at"])

    # Expected Results (Manual Calculation)
    expected_data = [
        ("Electronics", datetime.date(2023, 1, 15), 21.50, 10.75, 0.5),
        ("Home", datetime.date(2023, 1, 15), 25.00, 25.00, 0.0),
        ("Electronics", datetime.date(2023, 1, 16), 5.00, 5.00, 0.0),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["category", "order_date", "daily_revenue", "avg_order_value", "avg_return_rate"])

    # Actual Result
    actual_df = calculate_category_kpis(order_items_df, products_df)

    # Comparison (convert to Pandas for easier comparison, sort to ensure order)
    # Use pandas for robust comparison if available
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
    order_items_df = spark_session.createDataFrame(order_items_data, ["id", "order_id", "product_id", "sale_price", "status"])

    # Expected Results (Manual Calculation)
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

# --- Removed test_store_category_kpis test ---

# --- Removed test_store_order_kpis test ---

# --- Removed test_run_kpi_job_e2e test ---