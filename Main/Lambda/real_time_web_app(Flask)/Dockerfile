# Sử dụng một base image Python chính thức
FROM python:3.9-slim

# Đặt thư mục làm việc trong container
WORKDIR /app

# Sao chép file requirements.txt của Flask app
COPY ./requirements.txt .

# Cài đặt các thư viện
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ nội dung của thư mục Flask app vào thư mục làm việc trong container
COPY . .

# Expose cổng mà Flask app sẽ chạy (được cấu hình bởi FLASK_APP_PORT trong .env)
# CMD sẽ sử dụng biến môi trường này
# EXPOSE ${FLASK_APP_PORT} # Không cần thiết vì cổng được map trong docker-compose

# Lệnh để chạy ứng dụng khi container khởi động, sử dụng Gunicorn
# FLASK_APP_PORT sẽ được đọc từ environment variables (set via .env file in docker-compose)
# Set default for Gunicorn CMD if FLASK_APP_PORT is not passed from docker-compose .env
ENV FLASK_APP_PORT=${FLASK_APP_PORT:-5001}
CMD ["gunicorn", "--bind", "0.0.0.0:${FLASK_APP_PORT}", "app:app"] 