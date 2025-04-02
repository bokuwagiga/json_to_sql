# Contains SQL Server operations
import datetime
import re
import traceback
import pandas as pd
import pyodbc


class SqlServerTableCreator:
    """
    Creates SQL tables from normalized data structure and inserts records.

    Handles table creation with proper constraints and data type inference,
    while managing the complex parent-child relationships between tables.
    """

    def __init__(self, conn_str):
        """
        Initialize with a connection string to SQL Server.

        Args:
            conn_str: Connection string for SQL Server
        """
        self.conn_str = conn_str
        self.id_maps = {}  # Maps table IDs to database IDs

    def create_tables_and_insert_data(self, tables, entity_hierarchy, schema="dbo", root_table_name="rootTable"):
        """
        Create tables in SQL Server and insert data from children up to parents.

        Args:
            tables: Dictionary of normalized tables
            entity_hierarchy: Dict mapping entities to their parents
            schema: SQL Server schema name
            root_table_name: Name of the main/root table

        Returns:
            dict: Maps original IDs to database IDs for reference
        """
        # Use context manager to ensure proper connection handling
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()

            # Need to process tables in dependency order (children first)
            # so FK constraints don't fail when inserting
            processing_order = self._determine_processing_order(tables.keys(), entity_hierarchy)

            # Create and populate entity tables first
            # (these hold actual data, not relationships)
            for table_name in processing_order:
                if table_name in tables and not table_name.endswith('_rel'):
                    table = tables[table_name]
                    # Flag root table for special handling (IsCurrent column)
                    self._create_entity_table_if_not_exists(cursor, table_name, table, schema,
                                                            is_root_table=table_name == root_table_name)
                    self._insert_entity_data(cursor, table_name, table, schema)
                    conn.commit()  # Commit after each table to avoid long transactions

            # Now process junction/relationship tables that connect entities
            # These need the entity tables to exist first for FK constraints
            for table_name in tables.keys():
                if table_name.endswith('_rel'):
                    table = tables[table_name]
                    self._create_relationship_table_if_not_exists(cursor, table_name, table, schema)
                    self._insert_relationship_data(cursor, table_name, table, schema)
                    conn.commit()

        return self.id_maps  # Return ID mappings for reference

    def _determine_processing_order(self, table_names, entity_hierarchy):
        """
        Figures out the order to process tables (from children to parents).
        We need this because we have to create child tables first due to FK constraints.

        Args:
            table_names: All table names in our dataset
            entity_hierarchy: Dict showing which entity is a child of which parent

        Returns:
            list: Tables in proper processing order
        """
        # We only care about entity tables, not the relationship tables
        entity_tables = [name for name in table_names if not name.endswith('_rel')]

        # Create a dependency graph where parents point to their children
        # This is basically a directed graph representation
        graph = {entity: [] for entity in entity_tables}

        # Populate the graph based on parent-child relationships
        for child, parent in entity_hierarchy.items():
            if child in graph and parent in graph:
                # Parent depends on child (child must be created first)
                graph[parent].append(child)

        # Use topological sort to determine processing order
        # This is a pretty standard algo for dependency resolution
        visited = set()  # permanently marked nodes
        temp = set()  # temporarily marked nodes (for cycle detection)
        order = []  # final processing order

        def visit(node):
            # Skip if already processed
            if node in visited:
                return
            # Detect cycles (shouldn't happen in proper data but just in case)
            if node in temp:
                return

            temp.add(node)

            # Process all dependencies (children) first
            for child in graph.get(node, []):
                visit(child)

            # Mark as visited and add to result
            visited.add(node)
            order.append(node)
            temp.remove(node)

        # Process all nodes
        for node in graph:
            if node not in visited:
                visit(node)

        # Flip the order because we want children first, then parents
        # (Topological sort gives us the reverse of what we want)
        return list(reversed(order))

    def _create_entity_table_if_not_exists(self, cursor, table_name, table, schema="dbo", is_root_table=False):
        """
        Create entity table if it doesn't exist.

        Args:
            cursor: SQL Server cursor
            table_name: Table name
            table: table for the table
            schema: SQL Server schema
        """
        # Make sure table name is SQL-safe
        safe_table_name = self._make_sql_safe(table_name)

        columns = []
        added_columns = set()  # Track already added column names

        for row in table:
            for col, value in row.items():  # Iterate over key-value pairs
                if col not in added_columns:  # Ensure uniqueness
                    added_columns.add(col)

                    if col == 'id':
                        columns.insert(0, "[id] INT IDENTITY(1,1) PRIMARY KEY")
                    else:
                        sql_type = self._get_sql_type(value)  # Infer SQL type from value
                        safe_col_name = self._make_sql_safe(col)
                        columns.append(f"[{safe_col_name}] {sql_type}")

        # Create schema if it doesn't exist
        cursor.execute(
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA [{schema}]');"
        )
        # Create table if it doesn't exist
        create_table_sql = f"""
        IF NOT EXISTS (
            SELECT * FROM sys.objects 
            WHERE object_id = OBJECT_ID(N'[{schema}].[{safe_table_name}]') 
            AND type in (N'U')
        )
        BEGIN
            CREATE TABLE [{schema}].[{safe_table_name}] (
                {", ".join(columns)},
                {"[IsCurrent] INT DEFAULT 1," if is_root_table else ""}
                [Inserted] DATETIME DEFAULT GETDATE()
            )
        END
        """
        cursor.execute(create_table_sql)

    def _create_relationship_table_if_not_exists(self, cursor, table_name, table, schema="dbo"):
        """
        Create relationship table with proper foreign key constraints if it doesn't exist.

        Args:
            cursor: SQL Server cursor
            table_name: Name of the relationship table
            table: List of records for the table
            schema: SQL Server schema name
        """
        # SQL-safe table name to prevent injection
        safe_table_name = self._make_sql_safe(table_name)

        # Find all entities referenced in this relationship table
        entity_names = set()
        for row in table:
            for col in row.keys():
                if col.endswith('_id'):
                    # Extract the entity name by removing '_id' suffix
                    entity_names.add(col[:-3])

        # Prepare column definitions and constraints
        columns = []
        constraints = []

        # For each entity referenced, create a column and FK constraint
        for entity_name in entity_names:
            safe_entity_name = self._make_sql_safe(entity_name)
            col_name = f"{entity_name}_id"
            safe_col_name = self._make_sql_safe(col_name)

            # Add column definition
            columns.append(f"[{safe_col_name}] INT NOT NULL")

            # Add foreign key constraint
            constraints.append(f"""
                FOREIGN KEY ([{safe_col_name}]) 
                REFERENCES [{schema}].[{safe_entity_name}] ([id])
            """)

        # Create a composite primary key from all entity ID columns
        # This enforces uniqueness for the relationship combination
        primary_key_cols = [f"[{self._make_sql_safe(entity_name + '_id')}]" for entity_name in entity_names]
        constraints.append(f"CONSTRAINT [PK_{safe_table_name}] PRIMARY KEY ({', '.join(primary_key_cols)})")

        # Make sure the schema exists before creating the table
        cursor.execute(
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA [{schema}]');")

        # Only create the table if it doesn't already exist
        create_table_sql = f"""
        IF NOT EXISTS (
            SELECT * FROM sys.objects 
            WHERE object_id = OBJECT_ID(N'[{schema}].[{safe_table_name}]') 
            AND type in (N'U')
        )
        BEGIN
            CREATE TABLE [{schema}].[{safe_table_name}] (
                {", ".join(columns)},   
                {", ".join(constraints)},
                [Inserted] DATETIME DEFAULT GETDATE()
            )
        END
        """
        cursor.execute(create_table_sql)

    def _insert_entity_data(self, cursor, table_name, table, schema="dbo"):
        """
        Insert entity data into the table and map original IDs to database IDs.

        Args:
            cursor: SQL Server cursor
            table_name: Table name
            table: Table data to insert
            schema: SQL Server schema
        """
        # SQL injection prevention
        safe_table_name = self._make_sql_safe(table_name)

        # Track ID mappings between our temp IDs and actual DB IDs
        self.id_maps[table_name] = {}

        # Process each row separately to get back the inserted IDs
        for row in table:
            # Don't insert the ID column - SQL Server will auto-generate it
            columns = [col for col in row.keys() if col != 'id']

            if not columns:  # Edge case: empty tables need at least one column
                # Check if dummy column exists before adding it
                cursor.execute(
                    f"SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(N'[{schema}].[{safe_table_name}]') AND name = 'dummy'")
                if cursor.fetchone()[0] == 0:
                    cursor.execute(f"ALTER TABLE [{schema}].[{safe_table_name}] ADD dummy BIT NULL")

                # Just insert NULL to get an ID back
                insert_sql = f"INSERT INTO [{schema}].[{safe_table_name}] (dummy) OUTPUT INSERTED.ID VALUES (NULL);"
                try:
                    cursor.execute(insert_sql)
                except Exception as e:
                    # Log trace for debugging but let it continue
                    traceback.print_exc()
            else:
                # Format values for SQL insertion
                values = []
                for col in columns:
                    value = row[col]
                    # Handle different data types appropriately
                    if pd.isna(value):
                        values.append("NULL")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    elif isinstance(value, bool):
                        values.append("1" if value else "0")  # SQL uses 1/0 for bit values
                    else:
                        # Escape single quotes in strings (prevents SQL injection)
                        values.append(f"'{str(value).replace('\'', '\'\'')}'")

                # Make column names SQL-safe
                safe_columns = [f"[{self._make_sql_safe(col)}]" for col in columns]

                # Create the insert statement
                insert_sql = f"""
                INSERT INTO [{schema}].[{safe_table_name}] ({', '.join(safe_columns)})
                OUTPUT INSERTED.ID
                VALUES ({', '.join(values)});
                """

                # Try until success, with a reasonable maximum attempts limit to prevent infinite loops
                max_attempts = 10
                attempt_count = 0
                success = False
                resized_columns = set()  # Track which columns have already been resized

                while not success and attempt_count < max_attempts:
                    attempt_count += 1  # Always increment on each attempt
                    try:
                        cursor.execute(insert_sql)
                        success = True  # Mark as successful
                    except pyodbc.ProgrammingError as e:
                        # Handle missing columns by adding them dynamically
                        if 'Invalid column name' in str(e):
                            match = re.search(r"Invalid column name '(.+?)'", str(e))
                            if match:
                                column_name = match.group(1)
                                # Add the missing column with a reasonable default type
                                alter_table_sql = f"ALTER TABLE [{schema}].[{safe_table_name}] ADD [{column_name}] NVARCHAR(255) NULL"
                                cursor.execute(alter_table_sql)
                                # This was a productive adjustment, not a failure
                                attempt_count -= 1
                            else:
                                traceback.print_exc()
                                raise
                        else:
                            raise  # Not a column issue, so re-raise
                    except pyodbc.DataError as e:
                        # Handle string truncation errors by expanding column sizes
                        if 'String or binary data would be truncated' in str(e):
                            # Find the longest value that needs to fit (that hasn't been resized yet)
                            alter_column_length = 0
                            alter_column_name = None
                            for long_col, long_val in zip(safe_columns, values):
                                stripped_col = long_col.replace('[', '').replace(']', '')
                                if (isinstance(long_val, str) and long_val.startswith("'") and
                                        long_val.endswith("'") and stripped_col not in resized_columns):
                                    # Strip quotes for length calculation on string literals
                                    val_length = len(long_val) - 2
                                    if val_length > alter_column_length:
                                        alter_column_length = val_length
                                        alter_column_name = stripped_col

                            # If we've already tried all columns, use NVARCHAR(MAX) on all string columns
                            if alter_column_name is None:
                                for long_col, long_val in zip(safe_columns, values):
                                    stripped_col = long_col.replace('[', '').replace(']', '')
                                    if (isinstance(long_val, str) and long_val.startswith("'") and
                                            long_val.endswith("'")):
                                        alter_column_name = stripped_col
                                        alter_column_length = 'max'
                                        break

                            if alter_column_name:
                                resized_columns.add(alter_column_name)  # Mark this column as resized
                                attempt_count -= 1  # Productive adjustment
                            else:
                                # If we can't find any column to resize, this is an unexpected case
                                raise Exception("Unable to determine which column to resize for truncation error")

                        # Handle type conversion errors
                        elif 'Conversion failed when converting' in str(e):
                            # Find which value caused the problem
                            errored_value = str(e).split('to data type')[0].strip().rsplit(' ')[-1]
                            columns_copy = [str(col) for col in safe_columns]
                            # Find the problem column
                            errored_value_index = values.index(errored_value)
                            alter_column_name = columns_copy[errored_value_index].replace('[', '').replace(']', '')
                            alter_column_length = len(errored_value)
                            resized_columns.add(alter_column_name)  # Mark as resized
                            attempt_count -= 1  # Productive adjustment
                        else:
                            raise  # Not a data issue we can fix, so re-raise

                        # Choose an appropriate column size, escalating as needed
                        alter_column_length = (
                            255 if alter_column_length < 255 else
                            500 if alter_column_length < 500 else
                            1000 if alter_column_length < 1000 else
                            2000 if alter_column_length < 2000 else
                            'max'  # Last resort for huge values
                        )
                        # Change the column type to fit the data
                        alter_table_sql = f"ALTER TABLE [{schema}].[{safe_table_name}] ALTER COLUMN [{alter_column_name}] NVARCHAR({alter_column_length}) NULL"
                        cursor.execute(alter_table_sql)
                    except Exception as e:
                        # Catch-all for unexpected issues
                        traceback.print_exc()
                        raise e
                if not success:
                    raise Exception(f"Failed to insert data after {max_attempts} attempts")

            # Store mapping between original ID and database ID for later use
            db_id = int(cursor.fetchone()[0])
            original_id = int(row['id'])
            self.id_maps[table_name][original_id] = db_id

    def _insert_relationship_data(self, cursor, table_name, table, schema="dbo"):
        """
        Insert relationship data into the table, using the mapped IDs from entity tables.
        """
        # Sanitize table name for SQL security
        safe_table_name = self._make_sql_safe(table_name)

        # Find all entities referenced in this relationship table by looking for _id columns
        entity_names = set()
        for row in table:
            for col in row.keys():
                if col.endswith('_id'):
                    entity_names.add(col[:-3])  # Strip the _id suffix to get entity name
        # Need to convert to list for consistent ordering in SQL queries
        entity_names = list(entity_names)

        # For each relationship record, map the original IDs to the actual DB IDs we got when inserting entities
        for row in table:
            values = []
            for entity_name in entity_names:
                col_name = f"{entity_name}_id"
                # Need to convert to int since IDs in our data structures might be strings
                original_id = int(row[col_name])
                # Look up what database ID was assigned when we inserted this entity
                db_id = self.id_maps[entity_name][original_id]
                values.append(str(db_id))

            # Make column names SQL-safe and properly formatted with brackets
            safe_columns = [f"[{self._make_sql_safe(entity_name + '_id')}]" for entity_name in entity_names]

            # Build and execute the insert statement
            insert_sql = f"""
            INSERT INTO [{schema}].[{safe_table_name}] ({', '.join(safe_columns)})
            VALUES ({', '.join(values)});
            """
            cursor.execute(insert_sql)

    def _get_sql_type(self, value):
        """
        Determine appropriate SQL Server data type based on Python value type.

        Args:
            value: Sample value from the data

        Returns:
            str: SQL Server data type name
        """
        # Numbers get their native SQL types
        if isinstance(value, int):
            return 'INT'
        elif isinstance(value, float):
            return 'FLOAT'
        elif isinstance(value, bool):
            return 'BIT'  # SQL Server uses BIT for booleans (1=True, 0=False)
        elif isinstance(value, (datetime.datetime, datetime.date)):
            return 'DATETIME'
        else:
            # Most strings fit in 255 chars - we'll resize later if needed
            # For any other types, default to NVARCHAR
            # This handles None values and complex objects
            return 'NVARCHAR(255)'

    def _make_sql_safe(self, name):
        """
        Make a name SQL-safe by removing special characters.

        Args:
            name: Original name

        Returns:
            str: SQL-safe name
        """
        # Replace special characters with underscores to prevent SQL injection
        return re.sub(r'[^a-zA-Z0-9_]', '_', name)
