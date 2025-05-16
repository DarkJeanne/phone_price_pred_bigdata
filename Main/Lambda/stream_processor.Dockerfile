# Base image
FROM python:3.9-slim

WORKDIR /opt/app

# Set PYTHONPATH to include the WORKDIR for easier imports from root
ENV PYTHONPATH=/opt/app

COPY ./stream_processor_requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy ML models and related operations (if needed by any of the scripts)
# stream_pipeline -> ML_consumer -> transformation -> insert_data_hbase
# stream_pipeline also uses producer.py
COPY ./ML_operations ./ML_operations

# Copy Stream layer code
COPY ./Stream_layer ./Stream_layer

# Copy Stream_data (used by stream_pipeline.py for stream_data.csv)
COPY ./Stream_data ./Stream_data

# Copy producer.py (imported by stream_pipeline.py)
COPY ./producer.py .

# Copy transform.py (imported by ML_consumer.py from parent directory of Stream_layer)
COPY ./transform.py .

# Command to run the stream pipeline orchestrator
CMD ["python", "-u", "Stream_layer/stream_pipeline.py"] 