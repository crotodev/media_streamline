import requests
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid

from utils.cassandra import create_keyspace, create_table

with open('config.yaml', 'r') as f:
    config = yaml.load(f, yaml.Loader)

cassandra = config['cassandra']

auth = PlainTextAuthProvider(
    username=cassandra['username'],
    password=cassandra['password']
)

cluster = Cluster(contact_points=cassandra['contact_points'], auth_provider=auth)
session = cluster.connect()

create_keyspace(
    cassandra['keyspace'],
    cassandra['class'],
    cassandra['replication_factor'],
    session
)

columns = {
    'id': 'UUID PRIMARY KEY',
    'title': 'TEXT',
    'author': 'TEXT',
    'text': 'TEXT',
    'summary': 'TEXT',
    'url': 'TEXT',
    'source': 'TEXT',
    'published_at': 'TIMESTAMP',
    'scraped_at': 'TIMESTAMP',
    'sentiment': 'TEXT',
}

create_table(
    cassandra['tablename'],
    cassandra['keyspace'],
    columns,
    session
)

packages = f"{config['spark']['kafka_package']},{config['spark']['cassandra_package']}"

# Initialize the spark session
spark = (
    SparkSession.builder.appName("NewsProcessing")
    .config("spark.jars.packages", packages) \
    .config("spark.cassandra.connection.host", config['cassandra']['contact_points'][0]) \
    .getOrCreate()
)

# Specify the schema for the incoming data
schema = StructType(
    [
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("text", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("url", StringType(), True),
        StructField("source", StringType(), True),
        StructField("published_at", TimestampType(), True),
        StructField("scraped_at", TimestampType(), True),
    ]
)

# Set up spark to read from kafka news topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", config['kafka']["bootstrap_servers"])
    .option("subscribe", config['kafka']['topic']) \
    .option('failOnDataLoss', 'false')
    .load()
)

df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")


def get_sentiment(text: str) -> str:
    """
    Makes request to sentiment analysis API to get sentiment
    :param text: The text to get the sentiment of
    :return: The sentiment of the text
    """

    headers = {"Content-Type": "application/json"}
    r = requests.post(config['inference_api']['url'], json={'text': text},
                      headers=headers)  # Make request to sentiment analysis API
    result = r.json()  # Get the result

    # return the sentiment
    if result[0]['label'] == "LABEL_0":
        return "NEGATIVE"
    return "POSITIVE"


def generate_uuid() -> str:
    """
    Generates a UUID
    :return:
    """
    return str(uuid.uuid4())


# Create a udf to get the sentiment
get_sentiment_udf = udf(get_sentiment, StringType())

generate_uuid_udf = udf(generate_uuid, StringType())

# Apply the udf to the summary column and create a new column called sentiment
df = df.withColumn('id', generate_uuid_udf()).withColumn('sentiment', get_sentiment_udf(col('summary')))
df = df.select('id', *([col for col in df.columns if col != 'id']))

query1 = df.writeStream.outputMode("append").format("console").start()
query2 = df.writeStream.outputMode("append") \
    .format('org.apache.spark.sql.cassandra') \
    .option('checkpointLocation', 'checkpoint') \
    .options(table=cassandra['tablename'], keyspace=cassandra['keyspace']).start()

query1.awaitTermination()
# query2.awaitTermination()
