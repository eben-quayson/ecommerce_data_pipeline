from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CSV Column Validator") \
    .getOrCreate()

# Define S3 paths
s3_paths = {
    "order_items": "s3://ecom-bucket-gyenyame/Data/order_items/*.csv",
    "orders": "s3://ecom-bucket-gyenyame/Data/orders/*.csv",
    "products": "s3://ecom-bucket-gyenyame/Data/products/*.csv"
}

# Define expected columns
expected_columns = {
    "order_items": {"id", "order_id", "product_id", "quantity", "sale_price", "status", "created_at"},
    "orders": {"order_id", "user_id", "status", "created_at"},
    "products": {"id", "name", "category", "price"}
}

# Function to validate columns
def validate_columns(df, expected_cols, table_name):
    actual_cols = set(df.columns)
    missing = expected_cols - actual_cols
    unexpected = actual_cols - expected_cols

    print(f"\nValidating {table_name}...")
    if not missing and not unexpected:
        print("✅ All expected columns are present.")
    else:
        if missing:
            print(f"❌ Missing columns in {table_name}: {missing}")
        if unexpected:
            print(f"⚠️ Unexpected columns in {table_name}: {unexpected}")

# Read and validate each table
for table_name, path in s3_paths.items():
    try:
        df = spark.read.csv(path, header=True, inferSchema=True)
        validate_columns(df, expected_columns[table_name], table_name)
    except Exception as e:
        print(f"🚨 Failed to read {table_name} from {path}: {str(e)}")

# Stop Spark session
spark.stop()
