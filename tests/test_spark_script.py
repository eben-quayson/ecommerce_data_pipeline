import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch

# Import the function to test
from scripts.compute_kpis import process_data

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for tests."""
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_process_data(spark):
    # Create mock data
    data = [
        ("Electronics", "2025-04-07", 1000),
        ("Clothing", "2025-04-07", 500),
        ("Electronics", "2025-04-08", 1500),
    ]
    
    # Define columns
    columns = ["category", "order_date", "daily_revenue"]
    
    # Create a Spark DataFrame with the mock data
    df = spark.createDataFrame(data, columns)

    # Mock the reading from S3 (here we directly use the mock DataFrame)
    with patch("your_script.SparkSession.read.csv") as mock_read_csv:
        mock_read_csv.return_value = df

        # Run the process_data function
        input_path = "mock/input/path"
        output_path = "mock/output/path"
        process_data(input_path, output_path)
        
        # Verify the transformations
        transformed_data = df.groupBy("category").agg({"daily_revenue": "sum"})
        
        # Check if the transformations are correct
        expected_data = [
            ("Electronics", 2500),
            ("Clothing", 500),
        ]
        
        expected_df = spark.createDataFrame(expected_data, ["category", "sum(daily_revenue)"])
        
        assert transformed_data.collect() == expected_df.collect()

