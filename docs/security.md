# Security Considerations for the KPI Calculation Pipeline

This document outlines the security considerations for the KPI calculation pipeline.

## Key Security Principles

* **Principle of Least Privilege:** IAM roles and policies are configured to grant only the necessary permissions required for the pipeline to function.
* **No Hardcoded Credentials:** AWS access keys and secret keys are not hardcoded in the application code or environment variables. IAM roles are used for authentication and authorization.

## Security Measures

1.  **IAM Roles:**
    * An **ECS Task Role** is associated with the ECS task running the Spark application. This role grants the container permissions to interact with AWS services.
    * The IAM policy attached to this Task Role should include the following minimum permissions:
        * `s3:ListBucket` on the specific S3 bucket: `arn:aws:s3:::ecom-bucket-gyenyame`
        * `s3:GetObject` on the specific prefix within the S3 bucket containing the data: `arn:aws:s3:::ecom-bucket-gyenyame/Data/*`
        * `dynamodb:PutItem` and `dynamodb:BatchWriteItem` on the specific DynamoDB tables:
            * `arn:aws:dynamodb:eu-west-1:YOUR_ACCOUNT_ID:table/category_kpis_table` (Replace `YOUR_ACCOUNT_ID`)
            * `arn:aws:dynamodb:eu-west-1:YOUR_ACCOUNT_ID:table/order_kpis_table` (Replace `YOUR_ACCOUNT_ID`)
    * The **ECS Task Execution Role** (`arn:aws:iam::182399707265:role/ecsTaskExecutionRole`) is used by the ECS agent to pull container images and manage other resources on behalf of your task. This role typically has permissions related to ECR and CloudWatch Logs.

2.  **Network Security:**
    * **Security Groups:** The ECS tasks are likely associated with security groups that control the inbound and outbound traffic. Ensure that only necessary ports are open and that traffic is restricted to required sources and destinations. Outbound access to Maven Central (for downloading Spark dependencies) and AWS services (S3, DynamoDB) is required.
    * **VPC Configuration:** The pipeline runs within a Virtual Private Cloud (VPC), providing network isolation.

3.  **Secrets Management (Consideration):**
    * For more sensitive configuration parameters (if any), consider using AWS Secrets Manager or AWS Systems Manager Parameter Store to store and retrieve them securely instead of hardcoding them in the application or environment variables.

4.  **Code Security:**
    * Regular code reviews should be conducted to identify and address any potential security vulnerabilities in the application code.
    * Dependency scanning tools can be used to check for known vulnerabilities in the libraries used (e.g., PySpark, `boto3`, `hadoop-aws`).

5.  **Data Encryption:**
    * **Data at Rest:** Ensure that the S3 bucket and DynamoDB tables are configured to use encryption at rest (e.g., using KMS keys).
    * **Data in Transit:** Ensure that data transferred between components (e.g., between Spark and S3/DynamoDB) is encrypted in transit (e.g., using HTTPS).

6.  **Logging and Monitoring:**
    * ECS logs are configured to be sent to CloudWatch Logs, providing visibility into the pipeline's execution and potential security-related events.
    * Monitor CloudWatch metrics and alarms for any unusual activity or errors.

7.  **Regular Updates and Patching:**
    * Keep the base container image and all dependencies up to date with the latest security patches.

By implementing these security measures, the risk of unauthorized access and data breaches can be significantly reduced.