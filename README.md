# JSON to SQL Server ETL Decomposer

A powerful Python ETL pipeline that automatically decomposes nested JSON data into relational database tables and loads them into SQL Server with proper schema design, data types, and relationships.
## Features

- **Automatic JSON Decomposition**: Converts complex nested JSON structures into properly normalized relational tables
- **Schema Detection**: Intelligently analyzes JSON structure to identify entities and relationships
- **Data Type Inference**: Automatically detects appropriate SQL Server data types (`INT`, `FLOAT`, `BIT`, `DATETIME`, `NVARCHAR`) based on actual non-null values
- **Relationship Management**: Creates proper foreign key constraints and composite-PK junction tables between related tables
- **Temporal Data Management**: Automatically handles slowly changing dimensions with `IsCurrent` flags (root table) and `Inserted` timestamps (all tables)
- **Identity Management**: Generates auto-incrementing primary keys while preserving relational integrity
- **Error Handling**: Dynamically adjusts column widths (`NVARCHAR` 255 → 500 → 1000 → 2000 → MAX) and adds missing columns on the fly
- **Optimized Table Design**: Removes all-null columns and ensures normalized form compliance
- **SQL Injection Prevention**: Safely handles special characters in identifiers and values
- **Nested Array Support**: Arrays of arrays are preserved as JSON strings rather than silently dropped
- **Script Generation**: Generates a pure SQL script without requiring a live database connection

## Installation

```bash
# Clone the repository
git clone https://github.com/bokuwagiga/json-to-sql.git

# Install required dependencies
pip install pandas pyodbc

# Or install the package in editable mode
pip install -e .
```

## Requirements

- Python 3.6+
- pandas
- pyodbc
- SQL Server with ODBC Driver 17 for SQL Server installed

## Usage

### 🌐 Live Demo Using the Hosted App 
#### Try the app online
1. Go to [https://json2sql.streamlit.app/](https://json2sql.streamlit.app/)
2. Paste your JSON data in the text area, configure the options, and click "Generate SQL"
3. Copy the generated SQL script or download it for use in SQL Server

### Basic Example, View the `example_1.py` file

```python
import json
from JsonToSQL import process_json_to_sql_server

# Load your JSON data
with open('data.json', 'r') as f:
    json_data = json.load(f)

# Process JSON and load into SQL Server
tables, id_maps = process_json_to_sql_server(
    json_data=json_data,
    server="your_server",
    port="your_port",
    username="your_username",
    password="your_password",
    db="your_database",
    schema="dbo",
    root_table_name="YourRootTable"
)
```

### Using the Components Separately, View the `example_2.py` file

```python
import json
from JsonToSQL import JsonDecomposer, SqlServerTableCreator

# Load your JSON data
with open('data.json', 'r') as f:
    json_data = json.load(f)

# Define SQL Server credentials
server = "your_server"
port = "your_port"
username = "your_username"
password = "your_password"
db = "your_database"
schema = "dbo"
root_table_name = "YourRootTable"

# Step 1: Decompose JSON data into relational tables
tables, entity_hierarchy = JsonDecomposer.decompose_to_tables(
    json_data,
    root_table_name=root_table_name
)

# Step 2: Create tables in SQL Server and insert data
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={db};UID={username};PWD={password}"
creator = SqlServerTableCreator(conn_str)
id_maps = creator.create_tables_and_insert_data(
    tables,
    entity_hierarchy,
    schema="dbo",
    root_table_name="CustomerData"
)
```

### Using the Script Generation Feature, View the example_3.py file

```python
import json
from JsonToSQL import JsonDecomposer, SqlServerTableCreator

# Load your JSON data
with open('data.json', 'r') as f:
    json_data = json.load(f)

# Step 1: Decompose JSON data into relational tables
tables, entity_hierarchy = JsonDecomposer().decompose_to_tables(
    json_data,
    root_table_name="MyData"
)

# Step 2: Generate SQL scripts without database connection
creator = SqlServerTableCreator(collect_script=True)
sql_script = creator.create_tables_and_insert_data(
    tables,
    entity_hierarchy,
    schema="dbo",
    root_table_name="MyData"
)

# Save or display the generated SQL script
with open('generated_script.sql', 'w') as f:
    f.write(sql_script)

# Alternatively, print the script to console
print(sql_script)
```

## Architecture

The pipeline flows through four stages:

```
JSON Input
  → JsonStructureAnalyzer   (core/analyzer.py)      — recursive entity/relationship detection
  → TableBuilder            (core/table_builder.py)  — temp IDs → real IDs, junction tables
  → JsonDecomposer          (core/decomposer.py)     — orchestrates above two into (tables, hierarchy)
  → SqlServerTableCreator   (database/sql_writer.py) — DDL/DML execution or script generation
```

The public API (`main/json_to_sql.py`) wraps all of this in `process_json_to_sql_server()`.

### Return Values

`JsonDecomposer.decompose_to_tables()` returns `(tables, entity_hierarchy)`:
- `tables` — dict mapping table names to lists of row dicts; includes both entity tables and junction/relationship tables (named `<entity>_rel`)
- `entity_hierarchy` — dict mapping each child entity name to its parent entity name; used by `SqlServerTableCreator` to build FK references and determine insertion order

### Key Behaviors

**Every generated table gets:**
- `id INT IDENTITY(1,1)` — auto-incrementing surrogate primary key
- `Inserted DATETIME DEFAULT GETDATE()` — audit timestamp on all tables
- `IsCurrent INT DEFAULT 1` — slowly-changing dimension flag on the root table only

**Processing order:** Tables are inserted children-first (topological sort) to satisfy FK constraints before parent rows are inserted.

**Type inference:** Scans all rows for the first non-null value per column and maps it to `INT`, `FLOAT`, `BIT`, `DATETIME`, or `NVARCHAR(255)`. On string truncation errors, columns auto-widen: 255 → 500 → 1000 → 2000 → MAX.

**Script generation:** Pass `collect_script=True` to `SqlServerTableCreator` to capture all SQL as a string instead of executing against a live database.

**SQL safety:** Identifiers are sanitized via `_make_sql_safe()` (special characters stripped). Values use pyodbc parameterization to prevent SQL injection.

## How It Works

### 1. JSON Structure Analysis

The `JsonStructureAnalyzer` class examines your JSON data to identify:
- Entities (tables) — both nested objects and arrays
- Relationships between entities (parent-child hierarchies)
- Appropriate fields for each entity

**Entity naming convention**: every child entity is prefixed with its parent's name to prevent collisions when different parent objects share the same array key. For a root table named `my_root`:
- `my_root.orders` array → table `my_root_orders`
- `my_root_orders.items` array → table `my_root_orders_items`
- Junction tables follow the same scheme: `my_root_orders_rel`, `my_root_orders_items_rel`

### 2. Table Building

The `TableBuilder` class transforms the analyzed structure into:
- Proper relational tables with unique auto-incrementing IDs
- Optimized column design with all-null columns removed
- Tables processed from deepest children to root for correct FK dependency order

### 3. SQL Server Table Creation

The `SqlServerTableCreator` class:
- Creates tables with appropriate data types and constraints
- Establishes foreign key relationships
- Handles data insertion with proper ID mapping
- Dynamically adjusts column types as needed for data

## Key Components

- **JsonDecomposer**: Main entry point that orchestrates the decomposition process
- **JsonStructureAnalyzer**: Analyzes JSON to identify entities and relationships
- **TableBuilder**: Assigns final auto-incrementing IDs and builds junction tables for parent-child relationships
- **SqlServerTableCreator**: Handles SQL Server table creation and data insertion

## Limitations and Considerations

- If your JSON data contains a field named `id`, it is automatically renamed to `original_id` to avoid conflicting with the auto-generated surrogate primary key
- Entity and junction table names are prefixed with their parent entity names (e.g. `root_orders`, `root_orders_items`). Deep nesting produces longer names — SQL Server's 128-character identifier limit applies
- Arrays whose items are themselves arrays are preserved as JSON strings in a `value` column rather than being further normalized
- Data type inference picks the first non-null value per column; columns that are null across all rows are dropped
- SQL Server specific; adapting to other databases would require modifications to `SqlServerTableCreator`
- Large JSON documents may require significant memory for processing

## Testing

```bash
python -m pytest tests/test_bugs.py -v
```

The test suite contains 14 regression tests covering array name collisions, nested array handling, boolean type inference, leaf entity detection, null-first type inference, and relationship insert error propagation.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.