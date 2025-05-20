FROM apache/spark:3.5.1
USER root

# Install Python dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Tăng timeout và retry cho pip
RUN pip3 config set global.timeout 600 && \
    pip3 config set global.retries 10

# Chia nhỏ việc cài đặt gói để tránh timeout
RUN pip3 install --no-cache-dir pyspark 
RUN pip3 install --no-cache-dir pandas numpy python-dotenv
RUN pip3 install --no-cache-dir scikit-learn pymongo
RUN pip3 install --no-cache-dir matplotlib happybase kafka-python
RUN pip3 install --no-cache-dir xgboost
RUN pip3 install --no-cache-dir pyspark 

USER spark
WORKDIR /opt/spark
