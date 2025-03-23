import datetime
import json
import re
import traceback
from typing import Dict
import pandas as pd
import pyodbc


class JsonNormalizer:
    """
    Handles the normalization of JSON data into relational database tables.
    """

    @staticmethod
    def normalize_json_to_nf(json_data, root_table_name="rootTable"):
        """
        Convert any JSON structure to normalized tables following nf principles.
        Uses auto-incrementing integer IDs and builds from children to parents.

        Args:
            json_data: The JSON data to normalize (can be a dict or already parsed JSON)
            root_table_name: Name for the root entity table

        Returns:
            tuple: (optimized_tables, entity_hierarchy) where:
                - optimized_tables: Dictionary of tables where keys are table names and values are tables
                - entity_hierarchy: Dictionary mapping entities to their parents
        """
        # Parse JSON if it's a string
        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string provided")

        # Analyze the structure to identify all entities
        structure_analyzer = JsonStructureAnalyzer(root_table_name)
        structure_analyzer.analyze(json_data)

        # Create entity tables with auto-incrementing IDs
        table_builder = TableBuilder(structure_analyzer.entities, structure_analyzer.relationships)
        tables = table_builder.build_tables()

        # Optimize tables by removing redundant columns
        optimized_tables = TableBuilder.optimize_tables(tables)

        return optimized_tables, structure_analyzer.entity_hierarchy

class JsonStructureAnalyzer:
    """
    Analyzes JSON structure to identify entities and relationships.
    """

    def __init__(self, root_name="root"):
        self.root_name = root_name
        self.entities = {}  # Stores entity records
        self.relationships = {}  # Stores relationships between entities
        self.entity_hierarchy = {}  # Tracks parent-child relationships
        self.next_temp_id = {}  # Tracks the next available temp_id for each entity

    def analyze(self, json_data):
        """
        Analyze the JSON structure to identify entities and relationships.
        """
        if isinstance(json_data, dict):
            # Initialize root entity
            self._init_entity(self.root_name)
            root_temp_id = self._get_next_temp_id(self.root_name)
            root_record = {"temp_id": root_temp_id}

            for key, value in json_data.items():
                self._process_field(key, value, self.root_name, root_temp_id)
                if isinstance(value, (str, int, float, bool)) or value is None:
                    root_record[key] = value

            self.entities[self.root_name].append(root_record)

        elif isinstance(json_data, list):
            # Initialize root entity for list
            self._init_entity(self.root_name)

            for item in json_data:
                if isinstance(item, dict):
                    temp_id = self._get_next_temp_id(self.root_name)
                    record = {"temp_id": temp_id}

                    for key, value in item.items():
                        self._process_field(key, value, self.root_name, temp_id)
                        if isinstance(value, (str, int, float, bool)) or value is None:
                            record[key] = value

                    self.entities[self.root_name].append(record)
                elif isinstance(item, (str, int, float, bool)) or item is None:
                    # Handle primitive values in a list
                    temp_id = self._get_next_temp_id(self.root_name)
                    self.entities[self.root_name].append({"temp_id": temp_id, "value": item})

    def _init_entity(self, entity_name):
        """
        Sets up a new entity container if we haven't seen this entity before.
        We need this to create proper tables later on.
        """
        if entity_name not in self.entities:
            self.entities[entity_name] = []  # Empty list to store records
            self.next_temp_id[entity_name] = 1  # Start IDs at 1 for each entity

    def _init_relationship(self, rel_name, parent_entity, child_entity):
        """
        Creates a new relationship between entities if this relationship doesn't exist yet.
        Also tracks parent-child relationships to help with building FK constraints later.
        """
        if rel_name not in self.relationships:
            self.relationships[rel_name] = []  # Store relationship records
            self.entity_hierarchy[child_entity] = parent_entity  # Track who's the parent

    def _get_next_temp_id(self, entity_name):
        """
        Gets and increments the ID counter for an entity.
        We use temp_ids during processing before assigning real DB IDs.
        """
        temp_id = self.next_temp_id[entity_name]
        self.next_temp_id[entity_name] += 1  # Bump the counter for next time
        return temp_id

    def _process_field(self, key, value, parent_entity, parent_temp_id):
        """Process a single field from the JSON data, creating appropriate entities and relationships."""
        if isinstance(value, dict):
            # For nested objects, create a new entity and link to parent
            entity_name = f"{parent_entity}_{key}"
            self._init_entity(entity_name)

            # Create parent-child relationship
            rel_name = f"{parent_entity}_{entity_name}_rel"
            self._init_relationship(rel_name, parent_entity, entity_name)

            # Create new record for this entity with a unique ID
            temp_id = self._get_next_temp_id(entity_name)
            record = {"temp_id": temp_id}

            # Recursively process all fields in the nested object
            for nested_key, nested_value in value.items():
                self._process_field(nested_key, nested_value, entity_name, temp_id)
                # Store primitive values directly in this entity
                if isinstance(nested_value, (str, int, float, bool)) or nested_value is None:
                    record[nested_key] = nested_value

            # Add completed record to the entity table
            self.entities[entity_name].append(record)

            # Create relationship record linking parent and child
            self.relationships[rel_name].append({
                f"{parent_entity}_temp_id": parent_temp_id,
                f"{entity_name}_temp_id": temp_id
            })

        elif isinstance(value, list):
            # For arrays, create a child entity using the field name
            entity_name = key
            self._init_entity(entity_name)

            # Link array entity to its parent
            rel_name = f"{parent_entity}_{entity_name}_rel"
            self._init_relationship(rel_name, parent_entity, entity_name)

            # Process each item in the array
            for item in enumerate(value):
                if isinstance(item, dict):
                    # For objects in arrays, create a new entity record
                    temp_id = self._get_next_temp_id(entity_name)
                    record = {"temp_id": temp_id}

                    # Process each field in the object
                    for item_key, item_value in item.items():
                        self._process_field(item_key, item_value, entity_name, temp_id)
                        # Store primitive values directly
                        if isinstance(item_value, (str, int, float, bool)) or item_value is None:
                            record[item_key] = item_value

                    # Add the record to the entity table
                    self.entities[entity_name].append(record)

                    # Link array item to parent
                    self.relationships[rel_name].append({
                        f"{parent_entity}_temp_id": parent_temp_id,
                        f"{entity_name}_temp_id": temp_id
                    })
                elif isinstance(item, (str, int, float, bool)) or item is None:
                    # For primitive values in arrays, create simple records with 'value' field
                    temp_id = self._get_next_temp_id(entity_name)
                    self.entities[entity_name].append({"temp_id": temp_id, "value": item})

                    # Link primitive array item to parent
                    self.relationships[rel_name].append({
                        f"{parent_entity}_temp_id": parent_temp_id,
                        f"{entity_name}_temp_id": temp_id
                    })

class TableBuilder:
    """
    Builds tables from entities and relationships with auto-incrementing integer IDs.
    """

    def __init__(self, entities, relationships):
        # Store input data for processing
        self.entities = entities
        self.relationships = relationships
        # Output containers
        self.tables = {}  # Will hold final processed table data
        self.id_maps = {}  # Used to track how temp_ids map to real DB IDs

    def build_tables(self):
        """
        Build tables from entities and relationships, resolving IDs from deepest children to parents.
        """
        # Find all entities with no children (leaf nodes)
        leaf_entities = self._find_leaf_entities()

        # Process entities from leaves to root
        processed_entities = set()
        while leaf_entities:
            entity_name = leaf_entities.pop()
            if entity_name not in processed_entities:
                self._process_entity(entity_name)
                processed_entities.add(entity_name)

            # Find new leaf entities
            self._update_leaf_entities(leaf_entities, processed_entities)

        # Process any remaining entities
        remaining_entities = set(self.entities.keys()) - processed_entities
        for entity_name in remaining_entities:
            self._process_entity(entity_name)

        # Process relationships
        self._process_relationships()

        return self.tables

    def _find_leaf_entities(self):
        """
        Find entities that have no children (leaf nodes in the entity hierarchy).
        """
        all_entities = set(self.entities.keys())
        parents = set()

        # Identify parent entities by checking relationships
        for rel_data in self.relationships.values():
            for rel in rel_data:
                for key in rel.keys():
                    if key.endswith('_temp_id'):
                        parent_entity = key.rsplit('_temp_id', 1)[0]
                        parents.add(parent_entity)

        # Leaf entities are those not in the parents set
        return all_entities - parents

    def _update_leaf_entities(self, leaf_entities, processed_entities):
        """Update the list of leaf entities after processing."""
        # Check each entity to see if all its children have been processed
        for entity_name in self.entities.keys():
            if entity_name not in processed_entities:
                children = self._find_child_entities(entity_name)
                # If all children are processed, add the entity to leaf_entities
                if all(child in processed_entities for child in children):
                    leaf_entities.add(entity_name)

    def _find_child_entities(self, parent_entity):
        """Find all direct child entities of a parent entity."""
        children = set()

        # Loop through all relationship tables
        for rel_name, rel_data in self.relationships.items():
            # Check if this relationship involves the parent entity
            if rel_data and f"{parent_entity}_temp_id" in rel_data[0]:
                # For each relationship record in this table
                for rel in rel_data:
                    # Look for keys that reference other entities (not the parent)
                    for key in rel.keys():
                        # temp_id keys identify entities; we want children, not the parent itself
                        if key.endswith('_temp_id') and not key.startswith(parent_entity):
                            # Extract entity name by removing the _temp_id suffix
                            child_entity = key.rsplit('_temp_id', 1)[0]
                            children.add(child_entity)

        return children

    def _process_entity(self, entity_name):
        """Process an entity and create its table with auto-incrementing IDs."""
        records = self.entities[entity_name]
        id_map = {}  # Maps temp_ids to real DB IDs

        table_records = []
        for real_id, record in enumerate(records, start=1): # Start IDs at 1 (SQL standard)
            # Replace temp_id with real auto-increment ID
            # We need to pop it so it doesn't stay in the record
            temp_id = record.pop('temp_id')
            id_map[temp_id] = real_id

            # Create new record with proper ID and convert special values
            table_record = {'id': real_id}
            for key, value in record.items():
                # Convert booleans to 1/0 for SQL compatibility
                if value is True:
                    table_record[key] = 1
                elif value is False:
                    table_record[key] = 0
                # Handle empty values consistently
                elif value in [None, ""]:
                    table_record[key] = None
                else:
                    table_record[key] = value
            table_records.append(table_record)

        # Store processed records and ID mapping for later use
        self.tables[entity_name] = table_records
        self.id_maps[entity_name] = id_map

    def _process_relationships(self):
        """Process relationships and create junction tables with resolved IDs."""
        for rel_name, rel_data in self.relationships.items():
            if not rel_data:
                continue

            table_records = []
            for rel in rel_data:
                table_record = {}

                for key, temp_id in rel.items():
                    if key.endswith('_temp_id'):
                        entity_name = key.rsplit('_temp_id', 1)[0]
                        real_id = self.id_maps[entity_name][temp_id]
                        table_record[f"{entity_name}_id"] = real_id

                table_records.append(table_record)

            self.tables[rel_name] = table_records

    @staticmethod
    def optimize_tables(tables: Dict[str, list]) -> Dict[str, list]:
        """
        Optimize the tables by removing redundancy and ensuring NF compliance.

        Args:
            tables: Dictionary of tables where keys are table names and values are lists of records

        Returns:
            Dict: Optimized tables with redundant columns removed
        """
        optimized = {}

        # Process each table separately
        for name, rows in tables.items():
            if not rows:  # Skip empty tables
                continue

            # Get column names from first row (all rows should have same structure)
            all_columns = rows[0].keys()

            # Find columns that have at least one non-null value
            # Because We want to remove columns that are completely empty/null
            non_null_cols = {col for col in all_columns if any(row.get(col) is not None for row in rows)}

            # Create new table with only non-null columns
            optimized[name] = [
                {col: row.get(col, None) for col in non_null_cols}
                for row in rows
            ]

        return optimized


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

                # Try a few times in case the table needs modification
                for insert_attempt in range(3):
                    try:
                        cursor.execute(insert_sql)
                        break  # Success! Exit the retry loop
                    except pyodbc.ProgrammingError as e:
                        # Handle missing columns by adding them dynamically
                        if 'Invalid column name' in str(e):
                            match = re.search(r"Invalid column name '(.+?)'", str(e))
                            if match:
                                column_name = match.group(1)
                                # Add the missing column with a reasonable default type
                                alter_table_sql = f"ALTER TABLE [{schema}].[{safe_table_name}] ADD [{column_name}] NVARCHAR(255) NULL"
                                cursor.execute(alter_table_sql)
                                continue  # Try the insert again
                            else:
                                traceback.print_exc()
                                raise
                        else:
                            raise  # Not a column issue, so re-raise
                    except pyodbc.DataError as e:
                        # Handle string truncation errors by expanding column sizes
                        if 'String or binary data would be truncated' in str(e):
                            # Find the longest value that needs to fit
                            alter_column_length = 0
                            alter_column_name = None
                            for long_col, long_val in zip(safe_columns, values):
                                if isinstance(long_val, str) and len(long_val) > alter_column_length:
                                    alter_column_length = len(long_val)
                                    alter_column_name = long_col.replace('[', '').replace(']', '')
                        # Handle type conversion errors
                        elif 'Conversion failed when converting' in str(e):
                            # Find which value caused the problem
                            errored_value = str(e).split('to data type')[0].strip().rsplit(' ')[-1]
                            columns_copy = [str(col) for col in safe_columns]
                            # Find the problem column
                            errored_value_index = values.index(errored_value)
                            alter_column_name = columns_copy[errored_value_index].replace('[', '').replace(']', '')
                            alter_column_length = len(errored_value)
                        else:
                            raise  # Not a data issue we can fix, so re-raise

                        # Choose an appropriate column size, escalating as needed
                        alter_column_length = (
                            255 if alter_column_length < 255 else
                            500 if alter_column_length < 500 else
                            1000 if alter_column_length < 1000 else
                            'max'  # Last resort for huge values
                        )
                        # Change the column type to fit the data
                        alter_table_sql = f"ALTER TABLE [{schema}].[{safe_table_name}] ALTER COLUMN [{alter_column_name}] NVARCHAR({alter_column_length}) NULL"
                        cursor.execute(alter_table_sql)
                        continue  # Try the insert again with bigger column
                    except Exception as e:
                        # Catch-all for unexpected issues
                        traceback.print_exc()
                        raise e

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