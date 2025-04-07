# Start with a base image for PySpark
FROM openjdk:8-jdk-alpine

# Install necessary dependencies
RUN apk add --no-cache python3 py3-pip

# Install PySpark
RUN pip3 install pyspark boto3

# Set the working directory to where your scripts are stored
WORKDIR /app/scripts

# Copy the script from your local machine to the container
COPY scripts/compute_kpis.py /app/scripts/

# Set the entrypoint for the container to run the script
CMD ["python3", "compute_kpis.py"]
