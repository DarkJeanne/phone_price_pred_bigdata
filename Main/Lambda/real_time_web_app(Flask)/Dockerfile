# Use an official Python image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Set working directory
WORKDIR /app

# Cài đặt các gói phụ thuộc cần thiết
RUN apt-get update && apt-get install -y gcc g++ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt
COPY ./requirements.txt .

# Install dependencies
RUN pip install --timeout=600 --retries=20 --no-cache-dir -r requirements.txt

# Copy all app files
COPY . .

# Set Flask port (default 5001 if not specified)
ENV FLASK_APP_PORT=${FLASK_APP_PORT:-5001}

# Command to run the app using Gunicorn
CMD gunicorn --bind 0.0.0.0:5001 app:app 