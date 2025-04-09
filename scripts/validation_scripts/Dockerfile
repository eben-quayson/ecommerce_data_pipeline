# Start with a base image for PySpark
FROM public.ecr.aws/x8r1y9g6/pyspark:v9

# Install necessary dependencies
RUN apt-get update && apt-get install -y python3 python3-pip bash curl

# Install PySpark
RUN pip3 install pyspark boto3

# Set the working directory to where your scripts are stored
WORKDIR /app/scripts

# Copy the script from your local machine to the container
COPY validate.py /app/scripts/

# Set the entrypoint for the container to run the script
CMD ["python3", "validate.py"]
