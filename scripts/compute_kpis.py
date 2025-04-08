import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, avg, when, to_date, lit


if __name__ == "__main__":
    # This script is designed to run as a standalone program. It will not be executed if imported as a module.
    pass

    # Step 1: Initialize Spark Session
    spark = SparkSession.builder \
        .appName("DynamoDB Integration with Spark") \
        .getOrCreate()

    # Step 2: Load your data (example, replace with actual paths)
    orders_df = spark.read.csv("s3://ecom-bucket-gyenyame/Data/orders/*.csv", header=True, inferSchema=True)
    order_items_df = spark.read.csv("s3://ecom-bucket-gyenyame/Data/order_items/*.csv", header=True, inferSchema=True)
    products_df = spark.read.csv("s3://ecom-bucket-gyenyame/Data/products/*.csv", header=True, inferSchema=True)

    # Step 3: Process Data (your KPI calculations)
    # Calculate category KPIs

    items_with_category = order_items_df.join(
        products_df,
        order_items_df.product_id == products_df.id,
        "inner"
    )

    # Step 2: Extract order_date
    items_with_category = items_with_category.withColumn("order_date", to_date("created_at"))

    # Step 3: KPI Aggregations
    category_kpis = items_with_category.groupBy("category", "order_date").agg(
        sum("sale_price").alias("daily_revenue"),
        (sum("sale_price") / countDistinct("order_id")).alias("avg_order_value"),
        (sum(when(col("status") == "returned", 1).otherwise(0)) / countDistinct("order_id")).alias("avg_return_rate")
    )


    # Calculate order KPIs
    orders = orders_df.alias("orders")
    items = order_items_df.alias("items")

    # Extract order_date
    orders = orders.withColumn("order_date", to_date("created_at"))

    # Join
    orders_with_items = orders.join(items, col("orders.order_id") == col("items.order_id"), "left")

    # Use fully qualified column names in aggregation
    order_kpis = orders_with_items.groupBy("order_date").agg(
        countDistinct("orders.order_id").alias("total_orders"),
        sum("items.sale_price").alias("total_revenue"),
        countDistinct("items.id").alias("total_items_sold"),
        (sum(when(col("items.status") == "returned", 1).otherwise(0)) / countDistinct("orders.order_id")).alias("return_rate"),
        countDistinct("orders.user_id").alias("unique_customers")
    )



    # Step 4: Store Data in DynamoDB
    def store_category_kpis_in_dynamodb(category_kpis):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table("category_kpis_table")

        # Loop through the DataFrame rows and insert into DynamoDB
        for row in category_kpis.collect():
            item = {
                'category': row['category'],
                'order_date': row['order_date'],
                'daily_revenue': row['daily_revenue'],
                'avg_order_value': row['avg_order_value'],
                'avg_return_rate': row['avg_return_rate']
            }

            # Insert item into DynamoDB
            table.put_item(Item=item)

        print("Category-level KPIs inserted into DynamoDB successfully.")

    def store_order_kpis_in_dynamodb(order_kpis):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table("order_kpis_table")

        # Loop through the DataFrame rows and insert into DynamoDB
        for row in order_kpis.collect():
            item = {
                'order_date': row['order_date'],
                'total_orders': row['total_orders'],
                'total_revenue': row['total_revenue'],
                'total_items_sold': row['total_items_sold'],
                'return_rate': row['return_rate'],
                'unique_customers': row['unique_customers']
            }

            # Insert item into DynamoDB
            table.put_item(Item=item)

        print("Order-level KPIs inserted into DynamoDB successfully.")

    # Step 6: Call the functions to store both category and order-level KPIs
    store_category_kpis_in_dynamodb(category_kpis)
    store_order_kpis_in_dynamodb(order_kpis)

    # Stop the Spark session
    spark.stop()

