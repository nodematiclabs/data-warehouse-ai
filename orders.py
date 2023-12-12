import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import random
from datetime import datetime, timedelta

# Function to generate random date
def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

# Generate random data
n = 1000  # number of records
order_ids = range(1, n + 1)
customer_ids = np.random.randint(1, 500, n)
order_dates = [random_date(datetime(2020, 1, 1), datetime(2023, 1, 1)) for _ in range(n)]
amounts = np.random.uniform(10, 1000, n)
statuses = np.random.choice(['Pending', 'Completed', 'Cancelled'], n)

# Create a DataFrame
df = pd.DataFrame({
    'order_id': order_ids,
    'customer_id': customer_ids,
    'order_date': order_dates,
    'amount': amounts,
    'status': statuses
})

client = bigquery.Client()

# Define table schema
schema = [
    bigquery.SchemaField("order_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
]

# Create table
project_id = 'YOUR_PROJECT'
dataset_id = 'demonstration'
table_id = f"{project_id}.{dataset_id}.orders"
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # API request

# Insert data into the table
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for the job to complete

print("Table created and data inserted successfully.")
