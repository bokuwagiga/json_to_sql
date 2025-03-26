# Contains class for analyzing JSON structure

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
            for item in value:
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
