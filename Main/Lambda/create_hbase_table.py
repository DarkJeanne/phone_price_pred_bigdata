import happybase

# Connect to HBase
connection = happybase.Connection(host='hbase', port=9090, autoconnect=False)
connection.open()

# Print existing tables
print("Existing tables:")
for table in connection.tables():
    print(table.decode('utf-8'))

# Create the 'smartphone' table with column family 'info' if it doesn't exist
table_name = 'smartphone'  # Use string, not bytes
cf_name = 'info'  # Use string, not bytes

if table_name.encode('utf-8') not in connection.tables():
    print(f"Creating table: {table_name}")
    
    # Create table with column family
    # Use a dict with column family as key and empty dict as value
    families = {cf_name: {}}
    connection.create_table(table_name, families)
    
    print(f"Table {table_name} created successfully")
else:
    print(f"Table {table_name} already exists")

# Verify table was created
print("\nVerifying tables after operation:")
for table in connection.tables():
    print(table.decode('utf-8'))

connection.close() 