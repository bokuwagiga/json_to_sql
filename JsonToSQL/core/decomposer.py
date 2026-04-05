# Decomposes JSON into relational tables
import json
from .analyzer import JsonStructureAnalyzer
from .table_builder import TableBuilder


class JsonDecomposer:
    """
    Decomposes nested JSON data into a set of flat relational tables.
    Each nested object or array becomes its own table; junction tables
    are created for parent-child relationships.
    """

    @staticmethod
    def decompose_to_tables(json_data, root_table_name="rootTable"):
        """
        Convert any JSON structure into a set of relational tables.
        Uses auto-incrementing integer IDs and processes from deepest
        children up to the root to satisfy FK dependency order.

        Args:
            json_data: The JSON data to decompose (dict, list, or JSON string)
            root_table_name: Name for the root entity table

        Returns:
            tuple: (tables, entity_hierarchy) where:
                - tables: dict mapping table names to lists of row dicts
                - entity_hierarchy: dict mapping each child entity to its parent
        """
        # Parse JSON if it's a string
        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string provided")

        # Analyze the structure to identify all entities and relationships
        structure_analyzer = JsonStructureAnalyzer(root_table_name)
        structure_analyzer.analyze(json_data)

        # Build entity tables with auto-incrementing IDs
        table_builder = TableBuilder(
            structure_analyzer.entities,
            structure_analyzer.relationships,
            structure_analyzer.entity_hierarchy,
        )
        tables = table_builder.build_tables()

        # Remove all-null columns
        optimized_tables = TableBuilder.optimize_tables(tables)

        return optimized_tables, structure_analyzer.entity_hierarchy
