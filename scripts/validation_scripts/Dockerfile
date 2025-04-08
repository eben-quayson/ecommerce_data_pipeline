# Start with a base image for PySpark
FROM openjdk:8-jdk-alpine

# Install necessary dependencies
RUN apk add --no-cache python3 py3-pip bash

# Install PySpark
RUN pip3 install pyspark boto3

# Set the working directory to where your scripts are stored
WORKDIR /app/scripts

# Copy the script from your local machine to the container
COPY validate.py /app/scripts/

# Set the entrypoint for the container to run the script
CMD ["python3", "validate.py"]
