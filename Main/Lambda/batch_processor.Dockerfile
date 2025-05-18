FROM python:3.8-slim

WORKDIR /app
ENV PYTHONPATH=/app

# Install Java for HDFS client
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY ./batch_processor_requirements.txt ./requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY ./Batch_layer ./batch_layer
COPY ./Stream_data ./Stream_data
COPY ./transform.py .
COPY ./producer.py .

# Set working directory
WORKDIR /app/batch_layer

# Command to run the batch pipeline
CMD ["python", "batch_pipeline.py"] 