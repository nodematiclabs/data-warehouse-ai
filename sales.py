import pandas as pd
import numpy as np
from google.cloud import bigquery
import random
from datetime import datetime, timedelta

# Function to generate random date
def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

# Constants for order_id and product_id ranges
ORDER_ID_RANGE = 1000  # Assuming we have 1000 orders
PRODUCT_ID_RANGE = 500  # Assuming we have 500 products

# Generate random data for the sales table
n = 2000  # number of sales records
sale_ids = range(1, n + 1)
order_ids = np.random.randint(1, ORDER_ID_RANGE + 1, n)
product_ids = np.random.randint(1, PRODUCT_ID_RANGE + 1, n)
quantities = np.random.randint(1, 10, n)  # assuming 1 to 9 products per sale
sale_dates = [random_date(datetime(2020, 1, 1), datetime(2023, 1, 1)) for _ in range(n)]

# Create a DataFrame for the sales data
df = pd.DataFrame({
    'sale_id': sale_ids,
    'order_id': order_ids,
    'product_id': product_ids,
    'quantity': quantities,
    'sale_date': sale_dates
})

# BigQuery client setup
client = bigquery.Client()

# Define table schema for the sales table
schema = [
    bigquery.SchemaField("sale_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("sale_date", "DATE", mode="REQUIRED"),
]

# Create the sales table
project_id = 'YOUR_PROJECT'
dataset_id = 'demonstration'
table_id = f"{project_id}.{dataset_id}.sales"
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # API request

# Insert data into the sales table
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for the job to complete

print("Sales table created and data inserted successfully.")
