import pandas as pd
import numpy as np
from google.cloud import bigquery
import random
import string

# Function to generate random product names
def random_product_name(length=10):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

# Generate random data
n = 500  # number of records
product_ids = range(1, n + 1)
names = [random_product_name(10) for _ in range(n)]
prices = np.random.uniform(5, 200, n)
categories = np.random.choice(['Electronics', 'Clothing', 'Home', 'Toys', 'Books'], n)
stock_quantities = np.random.randint(0, 100, n)

# Create a DataFrame
df = pd.DataFrame({
    'product_id': product_ids,
    'name': names,
    'price': prices,
    'category': categories,
    'stock_quantity': stock_quantities
})

# BigQuery client setup
client = bigquery.Client()

# Define table schema
schema = [
    bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("stock_quantity", "INTEGER", mode="REQUIRED"),
]

# Create table
project_id = 'YOUR_PROJECT'
dataset_id = 'demonstration'
table_id = f"{project_id}.{dataset_id}.products"
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # API request

# Insert data into the table
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for the job to complete

print("Products table created and data inserted successfully.")
