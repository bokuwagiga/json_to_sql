import json
from core import JsonNormalizer
from database import SqlServerTableCreator
# Load your JSON data
with open("data.json", "r") as f:
    json_data = json.load(f)

# Define SQL Server credentials
server = "your_server"
port = "your_port"
username = "your_username"
password = "your_password"
db = "your_database"
schema = "dbo"
root_table_name = "YourRootTable"

# Step 1: Normalize JSON data into relational tables
tables, entity_hierarchy = JsonNormalizer().normalize_json_to_nf(
    json_data, root_table_name=root_table_name
)

# Step 2: Create tables in SQL Server and insert data
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={db};UID={username};PWD={password}"
creator = SqlServerTableCreator(conn_str)
id_maps = creator.create_tables_and_insert_data(
    tables, entity_hierarchy, schema=schema, root_table_name=root_table_name
)

print("JSON data successfully processed and inserted into SQL Server.")