# Example refactoring (within your original script file, e.g., process_kpis.py)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, countDistinct, sum, when, to_date
import boto3
from decimal import Decimal # Import Decimal for DynamoDB compatibility 


# --- Calculation Functions ---
def calculate_category_kpis(order_items_df: DataFrame, products_df: DataFrame) -> DataFrame:
    items_with_category = order_items_df.join(
        products_df,
        order_items_df.product_id == products_df.id,
        "inner"
    )

    items_with_category = items_with_category.withColumn("order_date", to_date(col("created_at"), "yyyy-MM-dd HH:mm:ss")) 

    category_kpis = items_with_category.groupBy("category", "order_date").agg(
        sum("sale_price").alias("daily_revenue"),
        (sum("sale_price") / countDistinct("order_id")).alias("avg_order_value"),
        (sum(when(col("status") == "returned", 1).otherwise(0)) / countDistinct("order_id")).alias("avg_return_rate")
    )
    return category_kpis

def calculate_order_kpis(orders_df: DataFrame, order_items_df: DataFrame) -> DataFrame:
    orders = orders_df.alias("orders")
    items = order_items_df.alias("items")

    
    orders = orders.withColumn("order_date", to_date(col("created_at"), "yyyy-MM-dd HH:mm:ss")) # Be explicit with format if known

    orders_with_items = orders.join(items, col("orders.order_id") == col("items.order_id"), "left")

    order_kpis = orders_with_items.groupBy("order_date").agg(
        countDistinct("orders.order_id").alias("total_orders"),
        sum("items.sale_price").alias("total_revenue"),
        countDistinct("items.id").alias("total_items_sold"), # Assuming items.id is unique order item line id
        (sum(when(col("items.status") == "returned", 1).otherwise(0)) / countDistinct("orders.order_id")).alias("return_rate"),
        countDistinct("orders.user_id").alias("unique_customers")
    )
    return order_kpis


def store_category_kpis_in_dynamodb(category_kpis_df: DataFrame, table_name: str):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    print(f"Attempting to write to DynamoDB table: {table_name}") # Add logging

    collected_rows = category_kpis_df.collect()
    print(f"Collected {len(collected_rows)} rows for category KPIs.")

    with table.batch_writer() as batch: # Use batch_writer for efficiency
        for row in collected_rows:
            # Convert floats/doubles to Decimal for DynamoDB, handle None/NaN
            item = {
                'category': row['category'],
                'order_date': row['order_date'].strftime('%Y-%m-%d'), # Store date as string YYYY-MM-DD
                'daily_revenue': Decimal(str(row['daily_revenue'])) if row['daily_revenue'] is not None else Decimal('0'),
                'avg_order_value': Decimal(str(row['avg_order_value'])) if row['avg_order_value'] is not None else Decimal('0'),
                'avg_return_rate': Decimal(str(row['avg_return_rate'])) if row['avg_return_rate'] is not None else Decimal('0')
            }
            print(f"Putting category item: {item}") # Add logging
            batch.put_item(Item=item)

    print(f"Category-level KPIs inserted into DynamoDB table {table_name} successfully.")


def store_order_kpis_in_dynamodb(order_kpis_df: DataFrame, table_name: str):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    print(f"Attempting to write to DynamoDB table: {table_name}") # Add logging

    collected_rows = order_kpis_df.collect()
    print(f"Collected {len(collected_rows)} rows for order KPIs.")

    with table.batch_writer() as batch: # Use batch_writer
        for row in collected_rows:
             # Convert floats/doubles/longs to Decimal/int for DynamoDB, handle None/NaN
            item = {
                'order_date': row['order_date'].strftime('%Y-%m-%d'), # Store date as string YYYY-MM-DD
                'total_orders': int(row['total_orders']) if row['total_orders'] is not None else 0,
                'total_revenue': Decimal(str(row['total_revenue'])) if row['total_revenue'] is not None else Decimal('0'),
                'total_items_sold': int(row['total_items_sold']) if row['total_items_sold'] is not None else 0,
                'return_rate': Decimal(str(row['return_rate'])) if row['return_rate'] is not None else Decimal('0'),
                'unique_customers': int(row['unique_customers']) if row['unique_customers'] is not None else 0
            }
            print(f"Putting order item: {item}") # Add logging
            batch.put_item(Item=item)

    print(f"Order-level KPIs inserted into DynamoDB table {table_name} successfully.")


# --- Main Execution Logic (can be in a main function) ---
def run_kpi_job(spark: SparkSession, s3_base_path: str, category_table: str, order_table: str):
    # Load data
    orders_df = spark.read.csv(f"{s3_base_path}/orders/*.csv", header=True, inferSchema=True)
    order_items_df = spark.read.csv(f"{s3_base_path}/order_items/*.csv", header=True, inferSchema=True)
    products_df = spark.read.csv(f"{s3_base_path}/products/*.csv", header=True, inferSchema=True)

    # Calculate KPIs
    category_kpis_result_df = calculate_category_kpis(order_items_df, products_df)
    order_kpis_result_df = calculate_order_kpis(orders_df, order_items_df)

    # Show results for debugging
    print("Category KPIs DataFrame Schema:")
    category_kpis_result_df.printSchema()
    print("Category KPIs Data:")
    category_kpis_result_df.show(truncate=False)

    print("Order KPIs DataFrame Schema:")
    order_kpis_result_df.printSchema()
    print("Order KPIs Data:")
    order_kpis_result_df.show(truncate=False)


    # Store KPIs
    store_category_kpis_in_dynamodb(category_kpis_result_df, category_table)
    store_order_kpis_in_dynamodb(order_kpis_result_df, order_table)



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DynamoDB Integration with Spark") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .getOrCreate()

    S3_BASE = "s3a://ecom-bucket-gyenyame/Data" # Or read from config
    CAT_TABLE = "category_kpis_table"
    ORD_TABLE = "order_kpis_table"

    run_kpi_job(spark, S3_BASE, CAT_TABLE, ORD_TABLE)

    spark.stop()