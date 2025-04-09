# E-commerce KPI Calculation Pipeline

## Overview

This project implements a data pipeline designed to calculate key performance indicators (KPIs) from e-commerce data. The pipeline reads raw data from Amazon S3, processes it using Apache Spark, and stores the resulting KPIs in Amazon DynamoDB for further analysis and reporting.

The primary goal of this pipeline is to provide insights into the performance of the e-commerce platform by calculating metrics such as daily revenue, average order value, return rates, total orders, and unique customers. These KPIs are calculated at both the category and order levels, offering a comprehensive view of business performance.

## Functionality

The pipeline performs the following key functions:

1.  **Data Ingestion:** Reads e-commerce data (orders, order items, and products) from CSV files stored in a designated Amazon S3 bucket.
2.  **Data Processing:** Utilizes Apache Spark for distributed processing and transformation of the raw data. This involves joining datasets, cleaning and formatting data, and performing aggregations to calculate the required KPIs.
3.  **KPI Calculation:** Implements specific business logic to calculate KPIs at two levels:
    * **Category Level:** Daily revenue, average order value, and average return rate per product category.
    * **Order Level:** Total orders, total revenue, total items sold, overall return rate, and the number of unique customers per day.
4.  **Data Storage:** Persists the calculated KPIs into two Amazon DynamoDB tables: `category_kpis_table` and `order_kpis_table`.

## Technologies Used

* **Apache Spark:** A powerful open-source distributed processing system used for large-scale data processing.
* **PySpark:** The Python API for Apache Spark, enabling the pipeline to be implemented in Python.
* **Amazon S3:** Scalable object storage used to store the raw e-commerce data.
* **Amazon DynamoDB:** A fully managed NoSQL database service used to store the processed KPIs.
* **Amazon ECS (Elastic Container Service):** A fully managed container orchestration service used to run the Spark application in a Docker container.
* **AWS Fargate:** A serverless compute engine for containers, allowing the pipeline to run without managing underlying infrastructure.
* **`boto3`:** The AWS SDK for Python, used within the Spark application to interact with Amazon DynamoDB.
* **Docker:** A platform for building, sharing, and running applications in containers.

## Setup (Brief Overview)

To run this pipeline, the following high-level steps are required:

1.  **Data Preparation:** Ensure the e-commerce data (orders, order items, products) is available in CSV format and stored in the designated S3 bucket under the `Data/` prefix.
2.  **IAM Configuration:** Configure an IAM role with the necessary permissions for the ECS task to access the S3 bucket (read access) and the DynamoDB tables (write access).
3.  **Containerization:** Build a Docker image containing the Spark application and its dependencies (including the `hadoop-aws` library for S3 connectivity).
4.  **ECS Task Definition:** Define an ECS task definition that specifies the container image, resource requirements, IAM role, and environment variables (including the AWS region).
5.  **ECS Service:** Create or update an ECS service to run the defined task definition.

## Contributing

Ebenezer Quayson - Data Engineer

## License

MIT LICENCE