import json
from JsonToSQL import process_json_to_sql_server

# Load your JSON data
with open("example.json", "r") as f:
    json_data = json.load(f)

# Define SQL Server credentials
server = "your_server"
port = "your_port"
username = "your_username"
password = "your_password"
db = "your_database"
schema = "dbo"
root_table_name = "YourRootTable"

# Basic Example: Process JSON to SQL Server in one step
process_json_to_sql_server(
    json_data, server, port, username, password, db, schema, root_table_name
)

print("JSON data successfully processed and inserted into SQL Server.")