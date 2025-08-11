# Contains class for building normalized tables
from typing import Dict

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

            # Collect all unique columns from all rows
            all_columns = set()
            for row in rows:
                all_columns.update(row.keys())

            # Find columns that have at least one non-null value
            # Because We want to remove columns that are completely empty/null
            non_null_cols = {col for col in all_columns if any(row.get(col) is not None for row in rows)}

            # Create new table with only non-null columns
            optimized[name] = [
                {col: row.get(col, None) for col in non_null_cols}
                for row in rows
            ]

        return optimized
