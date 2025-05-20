FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DEFAULT_TIMEOUT=300 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y gcc g++ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .

RUN pip install --timeout=600 --retries=20 --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_APP_PORT=${FLASK_APP_PORT:-5001}

CMD gunicorn --bind 0.0.0.0:5001 app:app 