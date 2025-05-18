#!/bin/bash
set -e

echo "Starting HBase Master..."
/entrypoint.sh master &
HBASE_PID=$!

# Wait for HBase to start
echo "Waiting for HBase Master to start..."
sleep 60

# Check if HBase Master is running
if ! ps -p $HBASE_PID > /dev/null; then
    echo "HBase Master failed to start, exiting"
    exit 1
fi

echo "Starting HBase Thrift server..."
# First try using hbase-daemon.sh
/hbase/bin/hbase-daemon.sh start thrift
sleep 10

# Check if Thrift server is running
if netstat -tulpn | grep 9090; then
    echo "HBase Thrift server is running and listening on port 9090"
else
    echo "First attempt to start Thrift server failed, trying alternative method..."
    # Try starting it in the background with full command
    nohup /hbase/bin/hbase thrift start -p 9090 > /hbase/logs/thrift-server.log 2>&1 &
    sleep 10
    
    # Check again
    if netstat -tulpn | grep 9090; then
        echo "HBase Thrift server started successfully on second attempt"
    else
        echo "WARNING: Failed to start Thrift server after multiple attempts"
        # Try one last method - direct java command
        nohup java -classpath $(hbase classpath) org.apache.hadoop.hbase.thrift.ThriftServer -p 9090 > /hbase/logs/thrift-direct.log 2>&1 &
        sleep 5
        echo "Attempted direct Java method as final fallback"
    fi
fi

echo "HBase and Thrift server startup completed."

# Create the smartphone table if it doesn't exist yet
echo "Creating HBase smartphone table if it doesn't exist..."
/hbase/bin/hbase shell -n << EOF
if not exists 'smartphone'
  create 'smartphone', 'info'
  puts "Created smartphone table"
else
  puts "Smartphone table already exists"
end
EOF

echo "Startup sequence completed. Keeping container running..."
# Keep container running
tail -f /dev/null 