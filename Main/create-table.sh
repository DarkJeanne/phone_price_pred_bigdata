#!/bin/bash
set -e

# Đợi HBase Master khởi động
echo "Waiting for HBase Master to start..."
sleep 30

# Tạo bảng smartphone với family info
echo "Creating smartphone table..."
echo "create 'smartphone', 'info'" | /hbase/bin/hbase shell

# Khởi động Thrift server
echo "Starting HBase Thrift server..."
/hbase/bin/hbase thrift start -f -b 0.0.0.0 -p 9090 &

# Đảm bảo Thrift server có thời gian khởi động
sleep 10
echo "HBase setup completed" 