from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CSV Column Validator") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .getOrCreate()

# Define S3 paths
s3_paths = {
    "order_items": "s3a://ecom-bucket-gyenyame/Data/order_items/*.csv",
    "orders": "s3a://ecom-bucket-gyenyame/Data/orders/*.csv",
    "products": "s3a://ecom-bucket-gyenyame/Data/products/*.csv"
}

# Define expected columns
expected_columns = {
    "order_items": {
        "id", "order_id", "user_id", "product_id", "status",
        "created_at", "shipped_at", "delivered_at", "returned_at", "sale_price"
    },
    "orders": {
        "order_id", "user_id", "status", "created_at",
        "returned_at", "shipped_at", "delivered_at", "num_of_item"
    },
    "products": {
        "id", "sku", "cost", "category", "name", "brand", "retail_price", "department"
    }
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
