# Contains the main entry point
from .normalizer import JsonNormalizer
from .sql_writer import SqlServerTableCreator

def process_json_to_sql_server(json_data, server, port, username, password, db, schema, root_table_name="rootTable"):
    """
    Processes JSON data and loads it into SQL Server tables.

    Args:
        json_data: The JSON data to process (string or dict)
        server: Server name or IP address
        port: port number
        username: Server username
        password: Server password
        db: Database name
        schema: SQL Server schema name
        root_table_name: Name for the root entity table (default: "rootTable")

    Returns:
        tuple: (tables, id_maps) where:
            - tables: Dict containing the normalized table data
            - id_maps: Dict mapping original IDs to database IDs
    """

    # Connection string for SQL Server
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={db};UID={username};PWD={password}"
    )

    # Transform the JSON into normalized tables
    tables, entity_hierarchy =  JsonNormalizer.normalize_json_to_nf(json_data, root_table_name)

    # Create the actual tables in SQL Server and load the data
    creator = SqlServerTableCreator(conn_str)
    id_maps = creator.create_tables_and_insert_data(
        tables, entity_hierarchy, schema, root_table_name=root_table_name
    )

    return tables, id_maps
