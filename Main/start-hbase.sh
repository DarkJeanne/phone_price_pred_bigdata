#!/bin/bash
set -e # Restoring set -e

echo "Sourcing HBase environment..."
# Attempt to source environment if the original entrypoint sets up anything crucial
# This is a guess; if /docker-entrypoint.sh exists and is relevant.
# Alternatively, rely on ENV variables set in Dockerfile or docker-compose.
# For harisekhon/hbase, /entrypoint.sh is the main script.
# We will call it with 'bash' to just get a shell if needed, or skip if not necessary.

echo "Starting HBase services (Master and RegionServer)..."
/hbase/bin/start-hbase.sh

# Wait for HBase Master and RegionServer to start
echo "Waiting for HBase services to initialize..."
sleep 60 # Increased wait time

# Check if HBase Master is running
echo "Checking if HBase Master is running..."
if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.master.HMaster"; then
    echo "ERROR: HBase Master failed to start, exiting."
    exit 1
fi
echo "HBase Master is running."

# Check if HBase RegionServer is running
echo "Checking if HBase RegionServer is running..."
if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.regionserver.HRegionServer"; then
    echo "ERROR: HBase RegionServer failed to start. Master might be up, but no RegionServer."
    exit 1 # Exiting if RegionServer isn't up after start-hbase.sh
fi
echo "HBase RegionServer is running."

# Create the smartphone table if it doesn't exist
echo "Creating HBase tables if they don't exist..."
sleep 25 # Combined original 15 + 10 delay

echo "Attempting 'hbase shell version' command..."
VERSION_OUTPUT=$(echo "version" | /hbase/bin/hbase shell -n 2>&1)
VERSION_EXIT_CODE=$?
echo "HBase shell 'version' exit code: $VERSION_EXIT_CODE"
echo "HBase shell 'version' output:"
echo "$VERSION_OUTPUT"

if [ $VERSION_EXIT_CODE -ne 0 ]; then
    echo "ERROR: 'hbase shell version' command failed. Check output above. Exiting."
    exit 1
fi

echo "Attempting to list tables via HBase shell..."
LIST_TABLES_OUTPUT=$(echo "list" | /hbase/bin/hbase shell -n 2>&1)
LIST_EXIT_CODE=$?
echo "HBase shell 'list' exit code: $LIST_EXIT_CODE"
echo "HBase shell 'list' output:"
echo "$LIST_TABLES_OUTPUT"

if [ $LIST_EXIT_CODE -ne 0 ]; then
    echo "ERROR: 'hbase shell list' command failed. Check output above. Will attempt to create table anyway but this is a bad sign."
fi

if echo "$LIST_TABLES_OUTPUT" | grep -q "TABLE"; then # A more generic check for list output
    if echo "$LIST_TABLES_OUTPUT" | grep -q "smartphone"; then
        echo "Smartphone table already exists."
    else
        echo "Smartphone table not found. Attempting to create table..."
        CREATE_TABLE_OUTPUT=$(echo "create 'smartphone', 'info'" | /hbase/bin/hbase shell -n 2>&1)
        CREATE_EXIT_CODE=$?
        echo "HBase shell 'create' exit code: $CREATE_EXIT_CODE"
        echo "HBase shell 'create' output:"
        echo "$CREATE_TABLE_OUTPUT"
        if [ $CREATE_EXIT_CODE -ne 0 ]; then
            echo "ERROR: HBase shell 'create' command failed."
            exit 1
        fi
        echo "Smartphone table creation attempt finished. Check logs for success."
    fi
else
    echo "WARNING: 'hbase shell list' did not return expected 'TABLE' keyword. Attempting to create table directly..."
    CREATE_TABLE_OUTPUT=$(echo "create 'smartphone', 'info'" | /hbase/bin/hbase shell -n 2>&1)
    CREATE_EXIT_CODE=$?
    echo "HBase shell 'create' exit code: $CREATE_EXIT_CODE"
    echo "HBase shell 'create' output:"
    echo "$CREATE_TABLE_OUTPUT"
    if [ $CREATE_EXIT_CODE -ne 0 ]; then
        echo "ERROR: HBase shell 'create' command failed during direct attempt."
        exit 1
    fi
    echo "Smartphone table direct creation attempt finished. Check logs for success."
fi


# Start the Thrift server
echo "Starting HBase Thrift server..."
/hbase/bin/hbase-daemon.sh start thrift -p 9090 --infoport 9096 -b 0.0.0.0 --localMasterPort 16000
# Added --infoport 9096 to avoid conflict if 9095 is used by REST
# Added --localMasterPort 16000 to help thrift find master

# Wait for Thrift to start
echo "Waiting for Thrift server to start..."
sleep 20 # Increased wait time

# Verify Thrift server is running
echo "Verifying Thrift server process..."
if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.thrift.ThriftServer"; then
    echo "ERROR: Thrift server process not found after start. Trying alternative start."
    # Fallback thrift start if the daemon script had issues
    /hbase/bin/hbase thrift start -p 9090 --infoport 9096 &
    sleep 15
    if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.thrift.ThriftServer"; then
       echo "ERROR: Alternative Thrift start also failed. Thrift server is not running."
       exit 1
    fi
fi
echo "HBase Thrift server process is running."

# Check Thrift port
echo "Checking Thrift server port 9090..."
if nc -z localhost 9090; then
    echo "Thrift server is listening on port 9090."
else
    echo "WARNING: Thrift server process might be running, but port 9090 is not accessible."
    # This could be a problem for client connections.
fi


echo "HBase startup script finished. Monitoring services (simulated by keeping script alive)..."
# Keep container running and monitor services (simplified)
while true; do
    # Basic check for Master
    if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.master.HMaster"; then
        echo "$(date): FATAL: HBase Master is NOT RUNNING. Exiting container to allow Docker to restart."
        exit 1 # Exit to allow Docker to handle restart
    fi
    
    # Basic check for RegionServer
    if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.regionserver.HRegionServer"; then
        echo "$(date): FATAL: HBase RegionServer is NOT RUNNING. Exiting container to allow Docker to restart."
        exit 1 # Exit to allow Docker to handle restart
    fi

    # Basic check for Thrift
    if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.thrift.ThriftServer"; then
        echo "$(date): WARNING: HBase Thrift Server is NOT RUNNING. Attempting to restart..."
        /hbase/bin/hbase-daemon.sh stop thrift # Stop first in case of stale pid
        sleep 5
        /hbase/bin/hbase-daemon.sh start thrift -p 9090 --infoport 9096 -b 0.0.0.0 --localMasterPort 16000
        sleep 15
        if ! ps -ef | grep -q "[o]rg.apache.hadoop.hbase.thrift.ThriftServer"; then
            echo "$(date): FATAL: Failed to restart Thrift server. Exiting."
            exit 1
        fi
        echo "$(date): Thrift server restarted."
    fi
        
    sleep 60 # Check interval
done

# Fallback to keep container alive if loop exits (should not happen with exit 1)
# tail -f /dev/null 