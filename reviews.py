import pandas as pd
import numpy as np
from google.cloud import bigquery
import random
from datetime import datetime, timedelta
import lorem  # For generating random text

# Function to generate random dates
def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

# Constants for product_id range
PRODUCT_ID_RANGE = 500  # Assuming we have 500 products

# Generate random data for the reviews table
n = 1000  # number of review records
review_ids = range(1, n + 1)
product_ids = np.random.randint(1, PRODUCT_ID_RANGE + 1, n)
customer_ids = np.random.randint(1, 500, n)  # Assuming 500 unique customers
ratings = np.random.randint(1, 6, n)  # Ratings from 1 to 5
review_texts = [lorem.paragraph() for _ in range(n)]
review_dates = [random_date(datetime(2018, 1, 1), datetime(2023, 1, 1)) for _ in range(n)]

# Create a DataFrame for the reviews data
df = pd.DataFrame({
    'review_id': review_ids,
    'product_id': product_ids,
    'customer_id': customer_ids,
    'rating': ratings,
    'review_text': review_texts,
    'review_date': review_dates
})

# BigQuery client setup
client = bigquery.Client()

# Define table schema for the reviews table
schema = [
    bigquery.SchemaField("review_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("rating", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("review_text", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("review_date", "DATE", mode="REQUIRED"),
]

# Create the reviews table
project_id = 'YOUR_PROJECT'
dataset_id = 'demonstration'
table_id = f"{project_id}.{dataset_id}.reviews"
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # API request

# Insert data into the reviews table
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for the job to complete

print("Reviews table created and data inserted successfully.")
