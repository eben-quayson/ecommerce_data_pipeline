# Architecture of the KPI Calculation Pipeline

This document outlines the architecture of the data pipeline responsible for calculating Key Performance Indicators (KPIs) from e-commerce data stored in Amazon S3 and persisting them in Amazon DynamoDB.

![Dashboard Screenshot](dashboard.png)


## Components

1.  **Data Source:**
    * **Amazon S3 Bucket:** `ecom-bucket-gyenyame`
    * **Data Location:** CSV files containing orders, order items, and products data are expected to be located under the `Data/` prefix within the S3 bucket.

2.  **Processing Engine:**
    * **Apache Spark:** A distributed processing framework used for reading, transforming, and aggregating the data to calculate KPIs.
    * **PySpark:** The Python API for Apache Spark, used to write the data processing logic.
    * **ECS Container:** The Spark application runs inside a Docker container managed by Amazon Elastic Container Service (ECS). The specific container image used is `182399707265.dkr.ecr.eu-west-1.amazonaws.com/ecr_repo_2:f2bac216031d3ff8fc696e76b986ec3140bb227b`.

3.  **Orchestration and Execution:**
    * **Amazon ECS (Elastic Container Service):** Manages the lifecycle of the Spark container, including starting, stopping, and scaling.
    * **Fargate:** The pipeline is likely running on AWS Fargate, providing serverless compute for containers.

4.  **Destination:**
    * **Amazon DynamoDB:** A fully managed NoSQL database service used to store the calculated KPIs.
    * **Tables:**
        * `category_kpis_table`: Stores KPIs calculated at the category level.
        * `order_kpis_table`: Stores KPIs calculated at the order level.

5.  **Programming Language and Libraries:**
    * **Python:** The primary programming language for the Spark application.
    * **PySpark:** For distributed data processing with Spark.
    * **`boto3`:** The AWS SDK for Python, used within the Spark application to interact with DynamoDB.

## Data Flow

1.  The ECS task starts the Spark application within the `ecom_container_2` container.
2.  The Spark application, using PySpark, reads the CSV files from the specified S3 bucket and prefix (`s3a://ecom-bucket-gyenyame/Data/`).
3.  Spark performs data transformations and aggregations as defined in the `calculate_category_kpis` and `calculate_order_kpis` functions.
4.  The resulting KPI DataFrames are then written to the respective DynamoDB tables (`category_kpis_table` and `order_kpis_table`) using the `boto3` library.

## Configuration

* **AWS Region:** The pipeline is configured to run in the `eu-west-1` AWS region (specified as an environment variable).
* **Spark Dependencies:** The `hadoop-aws` library (version 3.3.1) and its dependencies are downloaded by Spark at runtime using the `spark.jars.packages` configuration.
* **IAM Permissions:** The ECS task is configured with an IAM role that grants the necessary permissions to access the S3 bucket and DynamoDB tables.