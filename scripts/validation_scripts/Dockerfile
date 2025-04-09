# Start with a base image for PySpark
FROM public.ecr.aws/x8r1y9g6/pyspark:v9

# Install necessary dependencies
RUN apk add --no-cache python3 py3-pip bash

# Install PySpark
RUN pip3 install pyspark boto3

# Set the working directory to where your scripts are stored
WORKDIR /app/scripts

# Copy the script from your local machine to the container
COPY validate.py /app/scripts/

# Install curl for downloading JARs
RUN apt-get update && apt-get install -y  curl

ENV HADOOP_VERSION=3.2.2
ENV AWS_SDK_VERSION=1.11.1026

# Download and place JARs in Spark jars directory
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws-$HADOOP_VERSION.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/hadoop-aws-$HADOOP_VERSION.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_VERSION/aws-java-sdk-bundle-$AWS_SDK_VERSION.jar

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Set the entrypoint for the container to run the script
CMD ["python3", "validate.py"]
