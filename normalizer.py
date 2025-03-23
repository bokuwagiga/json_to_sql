# Contains main normalization logic
import json
from .analyzer import JsonStructureAnalyzer
from .table_builder import TableBuilder


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
