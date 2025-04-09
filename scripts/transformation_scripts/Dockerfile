# Use the existing PySpark base image
FROM public.ecr.aws/x8r1y9g6/pyspark:v9

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any required Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

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

# Run transform.py or validate.py depending on the container entrypoint
CMD ["python3", "compute_kpis.py"]