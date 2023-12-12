from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("BigQueryProductAnalytics") \
    .getOrCreate()

# Define a function for simple sentiment analysis
def simple_sentiment_analysis(description):
    positive_words = ['good', 'great', 'excellent', 'amazing', 'positive']
    negative_words = ['bad', 'poor', 'terrible', 'negative', 'worst']

    # Basic cleaning of the text
    cleaned_description = re.sub(r'[^a-zA-Z\s]', '', description).lower().split()

    # Simple sentiment analysis logic
    sentiment_score = sum(word in positive_words for word in cleaned_description) - \
                      sum(word in negative_words for word in cleaned_description)

    return 'Positive' if sentiment_score > 0 else 'Negative' if sentiment_score < 0 else 'Neutral'

# Register the UDF
sentiment_udf = udf(simple_sentiment_analysis, StringType())

# Read data from BigQuery
product_descriptions = spark.read.format('bigquery') \
    .option('table', 'YOUR_PROJECT.demonstration.reviews_en') \
    .load()

# Apply the sentiment analysis UDF
product_descriptions_with_sentiment = product_descriptions.withColumn('sentiment', sentiment_udf(product_descriptions['review_text']))

# Write the data back to a new BigQuery table
product_descriptions_with_sentiment.write.format('bigquery') \
    .option('table', 'YOUR_PROJECT.demonstration.sentiment') \
    .option("temporaryGcsBucket", 'sentiment-pyspark') \
    .mode('overwrite') \
    .save()

# Stop the Spark session
spark.stop()
