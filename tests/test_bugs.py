"""
Regression tests for bugs that were identified and fixed in the JsonToSQL package.

Each test documents the original bug it guards against:
  - ROOT CAUSE — where in the source the problem lived
  - WHY IT FAILED — the code path that triggered the wrong behaviour
  - HOW IT FAILED — the actual incorrect output that was produced
  - WHAT IS CORRECT — the behaviour that must hold after the fix (and must keep holding)

All 14 tests must pass on every commit.
"""

import unittest
from JsonToSQL.core.analyzer import JsonStructureAnalyzer
from JsonToSQL.core.table_builder import TableBuilder
from JsonToSQL.core.decomposer import JsonDecomposer
from JsonToSQL.database.sql_writer import SqlServerTableCreator


# ---------------------------------------------------------------------------
# Bug #3 — Array entity names don't include parent prefix, causing collisions
# ---------------------------------------------------------------------------

class TestArrayNameCollision(unittest.TestCase):
    """
    ROOT CAUSE (analyzer.py line 112-113)
    ======================================
    When a field value is a list, the child entity name is set to just the
    field key:
        entity_name = key          # e.g. "tags"

    When a field value is a dict, it is correctly scoped to the parent:
        entity_name = f"{parent_entity}_{key}"   # e.g. "orders_tags"

    Because the list branch does NOT prefix the parent name, any two parent
    objects that contain a list field with the same key will share one entity
    table and one set of IDs, silently mixing unrelated rows.
    """

    def test_same_array_key_under_different_parents_produces_separate_tables(self):
        """
        SCENARIO
        --------
        Input JSON has two top-level arrays ("orders" and "products"), each of
        whose objects contains a "tags" array:

            {
              "orders":   [{"order_id": 1,   "tags": ["urgent", "priority"]}],
              "products": [{"product_id": 1, "tags": ["electronics", "sale"]}]
            }

        WHY IT FAILS
        ------------
        analyzer._process_field("tags", [...], "orders", ...)  → entity_name = "tags"
        analyzer._process_field("tags", [...], "products", ...) → entity_name = "tags"  ← same!
        Both calls hit the same entity bucket.  _init_entity("tags") is a no-op on
        the second call because "tags" already exists, so all records pile into one table.

        HOW IT FAILS
        ------------
        tables produced:
          "tags": [urgent, priority, electronics, sale]   ← all 4 values mixed together
          "orders_tags_rel" and "products_tags_rel" both reference this single "tags" table

        WHAT IS CORRECT
        ---------------
        Each array should be scoped to its parent, producing:
          "orders_tags":   [urgent, priority]
          "products_tags": [electronics, sale]
        with separate relationship tables pointing to each.
        """
        json_data = {
            "orders":   [{"order_id": 1,   "tags": ["urgent", "priority"]}],
            "products": [{"product_id": 1, "tags": ["electronics", "sale"]}],
        }
        tables, _ = JsonDecomposer.decompose_to_tables(json_data, "root")

        self.assertNotIn(
            "tags", tables,
            "A single shared 'tags' table must not exist — "
            "each parent's array should produce its own scoped table."
        )
        # With parent-scoped naming: root_orders → root_orders_tags, root_products → root_products_tags
        self.assertIn("root_orders_tags",   tables, "'root_orders_tags' table is missing")
        self.assertIn("root_products_tags", tables, "'root_products_tags' table is missing")

    def test_data_not_mixed_across_colliding_array_tables(self):
        """
        SCENARIO
        --------
        Minimal version: two parents each with a "tags" array containing
        distinct single-letter values so mixing is immediately obvious.

            {
              "orders":   [{"tags": ["A", "B"]}],
              "products": [{"tags": ["C", "D"]}]
            }

        WHY IT FAILS
        ------------
        Same root cause as above.  Both "tags" arrays resolve to entity "tags".
        Every item appended from orders AND products lands in entities["tags"].

        HOW IT FAILS
        ------------
        tables["tags"] = [
            {"id": 1, "value": "A"},
            {"id": 2, "value": "B"},
            {"id": 3, "value": "C"},   ← belongs to products, not orders
            {"id": 4, "value": "D"},   ← belongs to products, not orders
        ]
        The test self.fail() is hit, printing the actual mixed list.

        WHAT IS CORRECT
        ---------------
        tables["orders_tags"]   = [{"id": 1, "value": "A"}, {"id": 2, "value": "B"}]
        tables["products_tags"] = [{"id": 1, "value": "C"}, {"id": 2, "value": "D"}]
        Each is isolated; no row from one parent appears in the other's table.
        """
        json_data = {
            "orders":   [{"tags": ["A", "B"]}],
            "products": [{"tags": ["C", "D"]}],
        }
        tables, _ = JsonDecomposer.decompose_to_tables(json_data, "root")

        if "tags" in tables:
            values = [row.get("value") for row in tables["tags"]]
            self.fail(
                f"Tags from different parents are mixed in one table: {values}. "
                "Expected separate 'orders_tags' and 'products_tags' tables."
            )


# ---------------------------------------------------------------------------
# Bug #4 — Arrays-of-arrays are silently dropped
# ---------------------------------------------------------------------------

class TestNestedArraysDropped(unittest.TestCase):
    """
    ROOT CAUSE (analyzer.py lines 119-150)
    =======================================
    When iterating over the items of a list, the code handles two cases:
        if isinstance(item, dict):   ...   # nested object → new entity
        elif isinstance(item, (str, int, float, bool)) or item is None:
                                     ...   # primitive → "value" column

    There is no branch for `isinstance(item, list)`.  Inner lists fall through
    both conditions silently — they are neither processed nor flagged.
    """

    def test_array_of_arrays_preserves_inner_lists(self):
        """
        SCENARIO
        --------
        A field whose value is a list of coordinate pairs (each pair is itself
        a list of two floats):

            {"coordinates": [[10.5, 20.3], [15.2, 25.8]]}

        WHY IT FAILS
        ------------
        _process_field("coordinates", [[10.5, 20.3], [15.2, 25.8]], "root", 1)
          → isinstance(value, list) is True → enters the list branch
          → for item [10.5, 20.3]:
              isinstance(item, dict)              → False
              isinstance(item, (str,int,...))     → False
              item is None                        → False
              → no branch matches → item is silently skipped

        Both inner lists are skipped.  entities["coordinates"] stays empty.
        optimize_tables() removes empty tables, so "coordinates" is gone entirely.

        HOW IT FAILS
        ------------
        "coordinates" is absent from tables:
            AssertionError: 'coordinates' table must exist

        WHAT IS CORRECT
        ---------------
        Each inner list should produce one row in "coordinates", e.g. stored as
        a JSON/string representation or as sub-columns (x, y).  At minimum the
        table must exist with 2 rows — one per coordinate pair.
        """
        json_data = {"coordinates": [[10.5, 20.3], [15.2, 25.8]]}
        tables, _ = JsonDecomposer.decompose_to_tables(json_data, "root")

        # After Bug #3 fix, array entity is scoped: "coordinates" → "root_coordinates"
        self.assertIn("root_coordinates", tables,
                      "'root_coordinates' table must exist")
        self.assertEqual(
            len(tables["root_coordinates"]), 2,
            "Both coordinate pairs must produce a row (2 rows expected)"
        )

    def test_mixed_array_preserves_list_items(self):
        """
        SCENARIO
        --------
        An array that contains a mix of a dict object and an inner list:

            {"data": [{"label": "point_a"}, [1.0, 2.0]]}

        WHY IT FAILS
        ------------
        The dict item {"label": "point_a"} is handled correctly and produces one row.
        The inner list [1.0, 2.0] hits no matching branch and is silently dropped.
        entities["data"] ends up with only 1 record instead of 2.

        HOW IT FAILS
        ------------
        len(tables["data"]) == 1, not 2:
            AssertionError: Both the object item and the list item must produce rows (2 rows expected)

        WHAT IS CORRECT
        ---------------
        Both items must produce rows.  The dict produces its normal record;
        the inner list should produce a row (e.g. stored as a string or with
        positional sub-columns).  Result: len(tables["data"]) == 2.
        """
        json_data = {
            "data": [
                {"label": "point_a"},
                [1.0, 2.0],
            ]
        }
        tables, _ = JsonDecomposer.decompose_to_tables(json_data, "root")

        # After Bug #3 fix, array entity is scoped: "data" → "root_data"
        self.assertIn("root_data", tables, "'root_data' table must exist")
        self.assertEqual(
            len(tables["root_data"]), 2,
            "Both the object item and the list item must produce rows (2 rows expected)"
        )


# ---------------------------------------------------------------------------
# Bug #1 — bool type check comes after int; BIT is never returned
# ---------------------------------------------------------------------------

class TestBoolSqlType(unittest.TestCase):
    """
    ROOT CAUSE (sql_writer.py lines 530-542)
    =========================================
    In Python, bool is a subclass of int.  isinstance(True, int) returns True.
    The current type-check order is:

        if isinstance(value, int):    return 'INT'    ← True matches here first
        elif isinstance(value, float):...
        elif isinstance(value, bool): return 'BIT'   ← dead code, never reached

    The bool branch must come BEFORE the int branch to be reachable.

    Note: TableBuilder already converts True/False to 1/0 before data reaches
    _create_entity_table_if_not_exists, so the column type in the DDL is wrong
    (INT instead of BIT) even though the inserted values are valid.
    """

    def setUp(self):
        self.creator = SqlServerTableCreator(collect_script=True)

    def test_true_maps_to_bit(self):
        """
        WHY IT FAILS
        ------------
        _get_sql_type(True):
            isinstance(True, int) → True  (because bool is a subclass of int)
            returns 'INT' immediately, never reaches the bool branch.

        HOW IT FAILS
        ------------
        assertEqual('INT', 'BIT') → AssertionError

        WHAT IS CORRECT
        ---------------
        Move the bool check before the int check:
            if isinstance(value, bool):  return 'BIT'
            elif isinstance(value, int): return 'INT'
        Then _get_sql_type(True) returns 'BIT'.
        """
        self.assertEqual(self.creator._get_sql_type(True), "BIT",
                         "_get_sql_type(True) must return 'BIT', not 'INT'")

    def test_false_maps_to_bit(self):
        """
        WHY IT FAILS  /  HOW IT FAILS  /  WHAT IS CORRECT
        ---------------------------------------------------
        Identical reasoning to test_true_maps_to_bit.
        False is also a bool (subclass of int), so isinstance(False, int) is True
        and 'INT' is returned instead of 'BIT'.
        """
        self.assertEqual(self.creator._get_sql_type(False), "BIT",
                         "_get_sql_type(False) must return 'BIT', not 'INT'")

    def test_integer_still_maps_to_int(self):
        """
        SANITY CHECK — must still pass after the fix.
        Plain integer literals (1, 0, 42) are not bool instances, so they must
        continue to map to 'INT' once the bool check is moved to the top.
        """
        self.assertEqual(self.creator._get_sql_type(1),  "INT")
        self.assertEqual(self.creator._get_sql_type(0),  "INT")
        self.assertEqual(self.creator._get_sql_type(42), "INT")

    def test_bool_column_uses_bit_in_generated_sql(self):
        """
        SCENARIO
        --------
        JSON with an explicit boolean field fed through the full pipeline:

            [{"name": "Alice", "is_active": True}]

        WHY IT FAILS
        ------------
        TableBuilder converts True → 1 (integer) before the table reaches
        _create_entity_table_if_not_exists.  _get_sql_type(1) returns 'INT'
        because the bool-before-int fix has not been applied.
        The column is declared as INT in the CREATE TABLE statement.

        HOW IT FAILS
        ------------
        Generated DDL:
            CREATE TABLE [dbo].[users] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                [name] NVARCHAR(255),
                [is_active] INT,          ← wrong type
                ...
            )
        assertIn("[is_active] BIT", sql) → AssertionError: '[is_active] BIT' not found

        WHAT IS CORRECT
        ---------------
        _get_sql_type must be called with the original boolean value (before the
        int conversion), or the bool check must be reordered so that BIT is
        inferred from the value 1 when it originates from a boolean field.
        The DDL should read: [is_active] BIT
        """
        json_data = [{"name": "Alice", "is_active": True}]
        tables, hierarchy = JsonDecomposer.decompose_to_tables(json_data, "users")

        creator = SqlServerTableCreator(collect_script=True)
        sql = creator.create_tables_and_insert_data(
            tables, hierarchy, schema="dbo", root_table_name="users"
        )

        self.assertIn("[is_active] BIT", sql,
                      "Boolean column 'is_active' must be declared as BIT in the DDL")
        self.assertNotIn("[is_active] INT", sql,
                         "Boolean column 'is_active' must not be declared as INT")


# ---------------------------------------------------------------------------
# Bug #2 — _find_leaf_entities marks both parent AND child as parents,
#           making the topological while-loop effectively dead
# ---------------------------------------------------------------------------

class TestLeafEntityDetection(unittest.TestCase):
    """
    ROOT CAUSE (table_builder.py lines 53-61)
    ==========================================
    _find_leaf_entities() is meant to return entities that have no children
    (leaf nodes), so the while-loop in build_tables() can process the hierarchy
    bottom-up.  The implementation iterates over every relationship record and
    extracts every key that ends in '_temp_id', adding the extracted name to
    a set called 'parents':

        for key in rel.keys():
            if key.endswith('_temp_id'):
                parents.add(key.rsplit('_temp_id', 1)[0])

    A relationship record contains TWO such keys — one for the parent entity
    and one for the child entity.  Both names are added to 'parents'.

    Result:
        parents = {all entity names that appear in any relationship}
        leaf_entities = all_entities - parents = {} (empty set)

    The while-loop `while leaf_entities:` never runs.  All entities fall into
    the `remaining_entities` fallback and are processed in arbitrary set order.
    The topological ordering is completely bypassed.
    """

    def test_child_entity_is_a_leaf(self):
        """
        SCENARIO
        --------
        Simple two-level hierarchy:

            {"products": [{"name": "Phone", "price": 999}]}

        entities:      root, products
        relationships: root_products_rel → [{"root_temp_id": 1, "products_temp_id": 1}]

        WHY IT FAILS
        ------------
        _find_leaf_entities() iterates the relationship record:
            key "root_temp_id"     → parents.add("root")
            key "products_temp_id" → parents.add("products")   ← child added as parent!

        parents = {"root", "products"}
        leaf_entities = {"root", "products"} - {"root", "products"} = set()

        HOW IT FAILS
        ------------
        assertIn("products", set()) → AssertionError:
            "'products' has no children — it must be identified as a leaf"

        WHAT IS CORRECT
        ---------------
        'parents' should only contain the PARENT side of each relationship.
        In the relationship record, the parent key is the one that corresponds
        to the table that OWNS the relationship (e.g. "root_temp_id").
        The child key ("products_temp_id") must not be added to 'parents'.

        Correct result:
            parents       = {"root"}
            leaf_entities = {"products"}
        """
        analyzer = JsonStructureAnalyzer("root")
        analyzer.analyze({"products": [{"name": "Phone", "price": 999}]})

        builder = TableBuilder(analyzer.entities, analyzer.relationships, analyzer.entity_hierarchy)
        leaves = builder._find_leaf_entities()

        # After Bug #3 fix, array entity names are scoped: "products" → "root_products"
        self.assertIn("root_products", leaves,
                      "'root_products' has no children — it must be identified as a leaf")
        self.assertNotIn("root", leaves,
                         "'root' has children — it must NOT be identified as a leaf")

    def test_deepest_entity_is_leaf_in_three_level_hierarchy(self):
        """
        SCENARIO
        --------
        Three-level hierarchy:

            {"orders": [{"order_id": 1, "items": [{"item_id": 1, "qty": 2}]}]}

        entities:      root, orders, items
        relationships: root_orders_rel, orders_items_rel

        WHY IT FAILS
        ------------
        All six temp_id keys across both relationship tables are added to 'parents':
            {"root", "orders", "items"}   ← every entity is in 'parents'

        leaf_entities = {} (empty)

        HOW IT FAILS
        ------------
        assertIn("items", set()) → AssertionError:
            "'items' is the deepest node and must be identified as a leaf"

        WHAT IS CORRECT
        ---------------
        parents       = {"root", "orders"}   (only the parent-side keys)
        leaf_entities = {"items"}
        The while-loop starts with "items", then once items is processed
        "orders" becomes a leaf, then "root" — correct bottom-up order.
        """
        analyzer = JsonStructureAnalyzer("root")
        analyzer.analyze({
            "orders": [{"order_id": 1, "items": [{"item_id": 1, "qty": 2}]}]
        })

        builder = TableBuilder(analyzer.entities, analyzer.relationships, analyzer.entity_hierarchy)
        leaves = builder._find_leaf_entities()

        # After Bug #3 fix, array entity names are scoped:
        # "orders" → "root_orders", "items" (inside root_orders) → "root_orders_items"
        self.assertIn("root_orders_items", leaves,
                      "'root_orders_items' is the deepest node and must be identified as a leaf")
        self.assertNotIn("root_orders", leaves,
                         "'root_orders' has 'root_orders_items' as a child — it must not be a leaf")
        self.assertNotIn("root", leaves,
                         "'root' has 'root_orders' as a child — it must not be a leaf")


# ---------------------------------------------------------------------------
# Bug #6 — Column type is inferred from the first row only;
#           a leading None causes int/float columns to be typed as NVARCHAR
# ---------------------------------------------------------------------------

class TestNullFirstValueTypeInference(unittest.TestCase):
    """
    ROOT CAUSE (sql_writer.py lines 218-227)
    =========================================
    _create_entity_table_if_not_exists() iterates over every row and for each
    column it encounters for the first time, calls _get_sql_type(value) on
    the value in THAT first row:

        for row in table:
            for col, value in row.items():
                if col not in added_columns:
                    added_columns.add(col)
                    sql_type = self._get_sql_type(value)   ← first-seen value only

    If the first row that contains a column has None as the value,
    _get_sql_type(None) returns 'NVARCHAR(255)'.  All subsequent rows
    that provide an actual integer or float for that column are ignored
    because the column is already in added_columns.

    The column is permanently typed as NVARCHAR regardless of the real data.
    """

    def _get_sql_for(self, json_data, root="people"):
        tables, hierarchy = JsonDecomposer.decompose_to_tables(json_data, root)
        creator = SqlServerTableCreator(collect_script=True)
        return creator.create_tables_and_insert_data(
            tables, hierarchy, schema="dbo", root_table_name=root
        )

    def test_integer_column_with_leading_null_stays_int(self):
        """
        SCENARIO
        --------
        Three people records where the first person has no age:

            [
                {"name": "Alice",   "age": None},
                {"name": "Bob",     "age": 30},
                {"name": "Charlie", "age": 25}
            ]

        WHY IT FAILS
        ------------
        Row 1 is processed first.  "age" is seen for the first time with value None.
        _get_sql_type(None) → 'NVARCHAR(255)'.  "age" is added to added_columns.
        Rows 2 and 3 have "age" = 30 and 25 respectively, but "age" is already
        in added_columns so _get_sql_type is never called again for that column.

        HOW IT FAILS
        ------------
        Generated DDL:
            CREATE TABLE [dbo].[people] (
                [id] INT IDENTITY(1,1) PRIMARY KEY,
                [age] NVARCHAR(255),    ← inferred from None, should be INT
                [name] NVARCHAR(255),
                ...
            )
        assertIn("[age] INT", sql) → AssertionError: '[age] INT' not found

        WHAT IS CORRECT
        ---------------
        The type-inference should scan all rows for a column and pick the first
        non-None value, or use the most common type.  With that fix:
            _get_sql_type(30) → 'INT'
        DDL: [age] INT
        """
        json_data = [
            {"name": "Alice",   "age": None},
            {"name": "Bob",     "age": 30},
            {"name": "Charlie", "age": 25},
        ]
        sql = self._get_sql_for(json_data)

        self.assertIn("[age] INT", sql,
                      "'age' column should be INT when values are integers (first row is NULL)")
        self.assertNotIn("[age] NVARCHAR", sql,
                         "'age' column must not be NVARCHAR just because the first row is NULL")

    def test_float_column_with_leading_null_stays_float(self):
        """
        SCENARIO
        --------
        Three product records where the first product has no price:

            [
                {"name": "Widget", "price": None},
                {"name": "Gadget", "price": 9.99},
                {"name": "Donut",  "price": 4.49}
            ]

        WHY IT FAILS
        ------------
        Same root cause as the integer test.  "price" is first seen with None,
        so it gets typed as NVARCHAR(255).  The float values 9.99 and 4.49 in
        subsequent rows are ignored for type-inference purposes.

        HOW IT FAILS
        ------------
        Generated DDL:
            [price] NVARCHAR(255)    ← should be FLOAT
        assertIn("[price] FLOAT", sql) → AssertionError: '[price] FLOAT' not found

        WHAT IS CORRECT
        ---------------
        The first non-None value (9.99) should determine the type.
        _get_sql_type(9.99) → 'FLOAT'
        DDL: [price] FLOAT
        """
        json_data = [
            {"name": "Widget", "price": None},
            {"name": "Gadget", "price": 9.99},
            {"name": "Donut",  "price": 4.49},
        ]
        sql = self._get_sql_for(json_data, root="products")

        self.assertIn("[price] FLOAT", sql,
                      "'price' column should be FLOAT when values are floats")
        self.assertNotIn("[price] NVARCHAR", sql,
                         "'price' column must not be NVARCHAR just because the first row is NULL")


# ---------------------------------------------------------------------------
# Bug #5 — Relationship insert failures are silently swallowed
# ---------------------------------------------------------------------------

class TestRelationshipSilentFailure(unittest.TestCase):
    """
    ROOT CAUSE (sql_writer.py lines 513-517)
    =========================================
    _insert_relationship_data() wraps every INSERT in a bare except block:

        try:
            cursor.execute(insert_sql)
        except Exception as e:
            print(f"Error inserting relationship: {e}")
            print(f"SQL: {insert_sql}")
            # exception is NOT re-raised

    Any DB error (FK violation, PK violation, connection failure) is printed
    and then silently discarded.  The caller receives no indication that any
    rows failed to insert.  The database ends up in a partially-loaded,
    inconsistent state with no error raised.
    """

    class _ErrorCursor:
        """Cursor that raises on any INSERT statement."""
        def execute(self, sql):
            if "INSERT INTO" in sql.upper():
                raise Exception("Simulated constraint violation")
        def fetchone(self):
            return [1]

    def test_relationship_insert_error_propagates(self):
        """
        SCENARIO
        --------
        A well-formed relationship row whose INSERT raises a DB-level exception
        (simulated here via a cursor that always raises on INSERT).

        WHY IT FAILS
        ------------
        The except block inside _insert_relationship_data catches the exception,
        prints two lines to stdout, and returns normally.
        assertRaises(Exception) receives no exception from the method → fails.

        HOW IT FAILS
        ------------
        Stdout output (visible in pytest -s):
            Error inserting relationship: Simulated constraint violation
            SQL: INSERT INTO [dbo].[orders_items_rel] ([orders_id], [items_id]) VALUES (1, 1)
        Test output:
            AssertionError: Exception not raised

        WHAT IS CORRECT
        ---------------
        The exception must be re-raised after (optionally) logging it:
            except Exception as e:
                print(...)
                raise          ← add this
        This lets the caller decide how to handle the failure and ensures the
        overall operation does not silently succeed when data was not loaded.
        """
        creator = SqlServerTableCreator(collect_script=False)
        creator.id_maps = {"orders": {1: 1}, "items": {1: 1}}

        rel_table = [{"orders_id": 1, "items_id": 1}]

        with self.assertRaises(Exception):
            creator._insert_relationship_data(
                self._ErrorCursor(), "orders_items_rel", rel_table, "dbo"
            )

    def test_missing_id_mapping_produces_incomplete_insert(self):
        """
        SCENARIO
        --------
        A relationship row that references "products_id", but "products" is
        absent from id_maps (e.g. because its entity table failed to load earlier).

            rel_table = [{"orders_id": 1, "products_id": 1}]
            id_maps   = {"orders": {1: 1}}   ← "products" missing

        WHY IT FAILS
        ------------
        The column-building loop in _insert_relationship_data:

            for col, value in row.items():
                if col.endswith('_id'):
                    entity_name = col[:-3]
                    if entity_name in self.id_maps and value in self.id_maps[entity_name]:
                        ...append column and value...

        "products" is not in id_maps, so the condition is False and "products_id"
        is silently skipped.  The INSERT is generated with only "orders_id".
        On a real SQL Server this INSERT would fail with a NOT NULL constraint
        violation on products_id (since it is declared NOT NULL in the rel table),
        but no error is ever raised here.

        HOW IT FAILS
        ------------
        inserted_sqls[0] =
            "INSERT INTO [dbo].[orders_products_rel] ([orders_id]) VALUES (1)"
                                                                   ^^^^^^^^^^
                                                      products_id is absent
        assertTrue("products_id" in inserted_sqls[0]) → AssertionError: False is not true

        WHAT IS CORRECT
        ---------------
        When a column referenced in the relationship row cannot be resolved from
        id_maps, the method must raise a descriptive error rather than building
        and executing a partial INSERT:
            raise KeyError(
                f"Entity '{entity_name}' not found in id_maps — "
                "relationship row cannot be fully resolved."
            )
        """
        creator = SqlServerTableCreator(collect_script=False)
        creator.id_maps = {"orders": {1: 1}}

        rel_table = [{"orders_id": 1, "products_id": 1}]

        inserted_sqls = []

        class TrackingCursor:
            def execute(self_, sql):      # noqa: N805
                inserted_sqls.append(sql)
            def fetchone(self_):          # noqa: N805
                return [1]

        # After the fix, a KeyError must be raised rather than silently dropping products_id
        with self.assertRaises(KeyError):
            creator._insert_relationship_data(
                TrackingCursor(), "orders_products_rel", rel_table, "dbo"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
