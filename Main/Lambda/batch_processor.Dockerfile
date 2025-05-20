FROM python:3.8-slim

WORKDIR /app
ENV PYTHONPATH=/app

# Install Java for HDFS client
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./batch_processor_requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY ./Batch_layer ./batch_layer
COPY ./Stream_data ./Stream_data
COPY ./transform.py .
COPY ./producer.py .

WORKDIR /app/batch_layer

CMD ["python", "batch_pipeline.py"] 