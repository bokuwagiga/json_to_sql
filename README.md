# JSON to SQL Server ETL Normalizer

A powerful Python ETL pipeline that automatically normalizes JSON data into relational database tables and loads them into SQL Server with proper schema design, data types, and relationships.
## Features

- **Automatic JSON Normalization**: Converts complex nested JSON structures into properly normalized relational tables
- **Schema Detection**: Intelligently analyzes JSON structure to identify entities and relationships
- **Data Type Inference**: Automatically detects appropriate SQL Server data types based on values
- **Relationship Management**: Creates proper foreign key constraints between related tables
- **Temporal Data Management**: Automatically handles slowly changing dimensions with IsCurrent flags (for the root table) and Inserted timestamps (for all tables)
- **Identity Management**: Generates auto-incrementing primary keys while preserving relational integrity
- **Error Handling**: Dynamically adjusts column types and handles edge cases
- **Optimized Table Design**: Removes redundant columns and ensures normalized form compliance
- **SQL Injection Prevention**: Safely handles special characters in names and values

## Installation

```bash
# Clone the repository
git clone https://github.com/bokuwagiga/json-to-sql.git

# Install required dependencies
pip install pandas pyodbc
```

## Requirements

- Python 3.6+
- pandas
- pyodbc
- SQL Server with ODBC Driver 17 for SQL Server installed

## Usage

### Basic Example, View the `example_1.py` file

```python
import json
from main import process_json_to_sql_server

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
from core import JsonNormalizer
from database import SqlServerTableCreator

# Step 1: Normalize JSON data into relational tables
tables, entity_hierarchy = JsonNormalizer.normalize_json_to_nf(
    json_data,
    root_table_name="CustomerData"
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

## How It Works

### 1. JSON Structure Analysis

The `JsonStructureAnalyzer` class examines your JSON data to identify:
- Entities (tables)
- Relationships between entities
- Appropriate fields for each entity
- Parent-child hierarchies

### 2. Table Building

The `TableBuilder` class transforms the analyzed structure into:
- Proper relational tables with unique IDs
- Optimized column design with redundant data removed
- Tables organized from children to parents for proper dependency order

### 3. SQL Server Table Creation

The `SqlServerTableCreator` class:
- Creates tables with appropriate data types and constraints
- Establishes foreign key relationships
- Handles data insertion with proper ID mapping
- Dynamically adjusts column types as needed for data

## Key Components

- **JsonNormalizer**: Main entry point that orchestrates the normalization process
- **JsonStructureAnalyzer**: Analyzes JSON to identify entities and relationships
- **TableBuilder**: Creates normalized table structures with proper IDs
- **SqlServerTableCreator**: Handles SQL Server table creation and data insertion

## Limitations and Considerations

- If your JSON data contains a field named 'ID', it must be renamed before processing as this name is reserved for auto-generated primary keys in the normalized tables
- Large JSON documents may require significant memory for processing
- The tool makes best-effort assumptions about data types based on sample values
- SQL Server specific; adapting to other databases would require slight modifications
- JSON documents with extremely complex nesting may generate many tables and too long table names

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.