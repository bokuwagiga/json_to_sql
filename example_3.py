from JsonToSQL import JsonNormalizer
from JsonToSQL import SqlServerTableCreator
import json

root_table_name = "root_table"
schema = "my_schema"

with open("example.json", "r") as f:
    json_data = json.load(f)
# Step 1: Normalize JSON data into relational tables
tables, entity_hierarchy = JsonNormalizer().normalize_json_to_nf(
    json_data, root_table_name=root_table_name
)

# Step 2: Generates SQL queries to create tables and insert data
creator = SqlServerTableCreator(collect_script=True)
sql_script = creator.create_tables_and_insert_data(tables, entity_hierarchy,schema=schema, root_table_name=root_table_name)
print(sql_script)


