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

# Run transform.py or validate.py depending on the container entrypoint
CMD ["python3", "compute_kpis.py"]