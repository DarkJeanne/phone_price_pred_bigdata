#!/bin/bash
set -e

# Đợi HBase Master khởi động
echo "Waiting for HBase Master to start..."
sleep 30

# Check if table already exists
echo "Checking if smartphone table exists..."
TABLE_EXISTS=$(/hbase/bin/hbase shell -n <<< "exists 'smartphone'" 2>/dev/null | grep "Table smartphone does exist")

if [ -z "$TABLE_EXISTS" ]; then
    # If table doesn't exist, create it
    echo "Creating smartphone table with column family 'info'..."
    /hbase/bin/hbase shell -n <<< "create 'smartphone', 'info'" 2>/dev/null
    
    if [ $? -ne 0 ]; then
        echo "Failed to create table, retrying..."
        sleep 5
        /hbase/bin/hbase shell -n <<< "create 'smartphone', 'info'" 2>/dev/null
    fi
else
    echo "Smartphone table already exists"
fi

# Verify table was created
echo "Verifying table exists..."
TABLE_EXISTS=$(/hbase/bin/hbase shell -n <<< "exists 'smartphone'" 2>/dev/null | grep "Table smartphone does exist")
if [ -n "$TABLE_EXISTS" ]; then
    echo "Smartphone table confirmed to exist"
else
    echo "WARNING: Could not confirm smartphone table exists"
fi

echo "HBase table setup completed" 