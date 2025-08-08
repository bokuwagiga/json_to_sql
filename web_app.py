import streamlit as st
import json
from core import JsonNormalizer
from database import SqlServerTableCreator
import traceback


def rename_id_fields(data, new_field_name="original_id"):
    """
    Recursively rename all 'id' fields in JSON data to avoid conflicts
    with SQL Server auto-increment ID columns.
    """
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            new_key = new_field_name if key == 'id' else key
            result[new_key] = rename_id_fields(value, new_field_name)
        return result
    elif isinstance(data, list):
        return [rename_id_fields(item, new_field_name) for item in data]
    else:
        return data


st.set_page_config(page_title="JSON to SQL Converter", page_icon="🔄")

st.title("JSON to SQL Converter")
st.write("Convert JSON data to SQL Server table creation and insertion scripts")

# Input form
with st.form("json_converter"):
    # JSON input with example data
    default_json = """{
  "users": [
    {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com",
      "orders": [
        {
          "id": 101,
          "product": "Widget",
          "price": 29.99,
          "quantity": 2
        }
      ]
    }
  ]
}"""

    json_text = st.text_area(
        "Paste your JSON data here:",
        height=300,
        placeholder="Enter your JSON data here...",
        help="Paste valid JSON data that you want to convert to SQL tables"
    )

    # Configuration options
    col1, col2, col3 = st.columns(3)
    with col1:
        root_table_name = st.text_input("Root table name:", value="root_table")
    with col2:
        schema = st.text_input("Schema name:", value="my_schema")
    with col3:
        rename_ids = st.checkbox("Rename 'id' fields", value=True,
                                 help="Rename 'id' fields to 'original_id' to avoid conflicts")

    # Submit button
    submit_button = st.form_submit_button("Generate SQL")

# Process the input when the form is submitted
if submit_button:
    # Check if JSON text is provided
    if not json_text or json_text.strip() == "":
        st.error("Please enter JSON data before generating SQL.")
    else:
        try:
            # Parse JSON input
            json_data = json.loads(json_text)

            # Optional: Rename 'id' fields to avoid conflicts
            if rename_ids:
                json_data = rename_id_fields(json_data, "original_id")
                st.info("Renamed all 'id' fields to 'original_id' to avoid SQL conflicts.")

            # Process the JSON data
            with st.spinner("Generating SQL..."):
                # Step 1: Normalize JSON data into relational tables
                tables, entity_hierarchy = JsonNormalizer().normalize_json_to_nf(
                    json_data, root_table_name=root_table_name
                )

                # Step 2: Generate SQL queries
                creator = SqlServerTableCreator(collect_script=True)
                sql_script = creator.create_tables_and_insert_data(
                    tables, entity_hierarchy, schema=schema, root_table_name=root_table_name
                )

            # Display the result
            st.success("SQL generated successfully!")

            # Show a preview of the tables created
            st.subheader("Tables Created:")
            table_names = [name for name in tables.keys()]
            st.write(f"Generated {len(table_names)} tables: {', '.join(table_names)}")

            # Show SQL in a code block for easy copying
            st.subheader("Generated SQL Script:")
            st.code(sql_script, language="sql")

            # Add a download button
            st.download_button(
                label="📥 Download SQL Script",
                data=sql_script,
                file_name=f"{schema}_{root_table_name}_script.sql",
                mime="text/plain"
            )

        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON format. Please check your input.\n\nError: {str(e)}")
            st.info("💡 Tip: You can use online JSON validators to check your JSON format.")

        except Exception as e:
            st.error(f"An error occurred while processing: {str(e)}")

            # Show detailed error in expander for debugging
            with st.expander("Show detailed error (for debugging)"):
                st.code(traceback.format_exc())

# Add usage instructions and example
with st.expander("📖 How to use this tool"):
    st.markdown("""
    ### Steps:
    1. **Paste your JSON data** in the text area above
    2. **Configure options**:
       - Set the root table name (main table name)
       - Set the schema name for your database
       - Choose whether to rename 'id' fields (recommended)
    3. **Click "Generate SQL"** to process the data
    4. **Copy or download** the generated SQL script

    ### Tips:
    - Ensure your JSON is valid before submitting
    - The tool automatically creates normalized tables from nested JSON
    - Relationship tables are created for nested arrays
    - Enable "Rename 'id' fields" to avoid conflicts with auto-increment IDs
    """)

with st.expander("📝 Example JSON"):
    st.code(default_json, language="json")
    if st.button("Use Example JSON"):
        st.rerun()

with st.expander("📝 Source Code"):
    st.markdown("[View Source Code on GitHub](https://github.com/bokuwagiga/json_to_sql)")
    st.markdown("Feel free to contribute or report issues!")

# Add footer
st.markdown("---")
st.markdown("*Built with Streamlit • JSON to SQL Converter*")