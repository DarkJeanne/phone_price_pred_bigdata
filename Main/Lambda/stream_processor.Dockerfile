FROM python:3.9-slim

WORKDIR /opt/app
ENV PYTHONPATH=/opt/app

ENV PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Cài đặt các gói phụ thuộc cần thiết
RUN apt-get update && apt-get install -y gcc g++ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY ./stream_processor_requirements.txt ./requirements.txt
RUN pip install --timeout=600 --retries=20 --no-cache-dir -r requirements.txt
# Cài đặt happybase riêng với các tùy chọn
RUN pip install --timeout=600 --retries=20 --no-cache-dir happybase==1.2.0

COPY ./ML_operations ./ML_operations
COPY ./Stream_layer ./Stream_layer
COPY ./Stream_data ./Stream_data
COPY ./producer.py .
COPY ./transform.py .

CMD ["python", "-m", "Stream_layer.stream_pipeline"]
