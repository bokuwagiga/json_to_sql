json_data = {
  "carts": [
    {
      "original_id": 1,
      "products": [
        {
          "original_id": 168,
          "title": "Charger SXT RWD",
          "price": 32999.99,
          "quantity": 3,
          "total": 98999.97,
          "discountPercentage": 13.39,
          "discountedTotal": 85743.87
          
        },
        {
          "original_id": 78,
          "title": "Apple MacBook Pro 14 Inch Space Grey",
          "price": 1999.99,
          "quantity": 2,
          "total": 3999.98,
          "discountPercentage": 18.52,
          "discountedTotal": 3259.18
          
        },
      ],
      "total": 103774.85,
      "discountedTotal": 89686.65,
      "userId": 33,
      "totalProducts": 4,
      "totalQuantity": 15
    },
    {
      "original_id": 2,
      "products": [
        {
          "original_id": 144,
          "title": "Cricket Helmet",
          "price": 44.99,
          "quantity": 4,
          "total": 179.96,
          "discountPercentage": 11.47,
          "discountedTotal": 159.32
          
        },
        {
          "original_id": 124,
          "title": "iPhone X",
          "price": 899.99,
          "quantity": 4,
          "total": 3599.96,
          "discountPercentage": 8.03,
          "discountedTotal": 3310.88
          
        }
      ],
      "total": 4794.8,
      "discountedTotal": 4288.95,
      "userId": 142,
      "totalProducts": 5,
      "totalQuantity": 20
    }
  ]
}
from core import JsonNormalizer
from database import SqlServerTableCreator


root_table_name = "root_table"
schema = "my_schema"

# Step 1: Normalize JSON data into relational tables
tables, entity_hierarchy = JsonNormalizer().normalize_json_to_nf(
    json_data, root_table_name=root_table_name
)

# Step 2: Generates SQL queries to create tables and insert data
creator = SqlServerTableCreator(collect_script=True)
sql_script = creator.create_tables_and_insert_data(tables, entity_hierarchy,schema=schema, root_table_name=root_table_name)
print(sql_script)


