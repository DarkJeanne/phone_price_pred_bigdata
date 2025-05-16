# Base image
FROM python:3.9-slim

WORKDIR /opt/app

# Set PYTHONPATH to include the WORKDIR for easier imports from root
ENV PYTHONPATH=/opt/app

# Install system dependencies that might be needed by hdfs client or other libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY ./data_ingestion_pipeline_requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/app/requirements.txt

# Remove old requirement handling and separate installs
# COPY ./producer_requirements.txt ./producer_requirements.txt
# COPY ./hdfs_consumer_requirements.txt ./hdfs_consumer_requirements.txt
# RUN pip install --no-cache-dir -r producer_requirements.txt -r hdfs_consumer_requirements.txt
# RUN pip install --no-cache-dir pandas hdfs

# Copy các scripts và thư mục cần thiết
COPY ./producer.py /opt/app/producer.py
COPY ./Batch_layer /opt/app/Batch_layer
COPY ./Stream_data /opt/app/Stream_data
# File batch_pipeline.py nằm trong Batch_layer theo cấu trúc mới

# Ensure batch_pipeline.py is executable if needed, though python interpreter usually handles this
# RUN chmod +x /opt/app/Batch_layer/batch_pipeline.py

# Command to run the batch pipeline orchestrator
# File batch_pipeline.py nằm trong thư mục Batch_layer sau khi copy
CMD ["python", "-u", "Batch_layer/batch_pipeline.py"] 