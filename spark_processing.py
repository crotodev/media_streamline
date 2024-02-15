import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType

# initialize the spark session
spark = (
    SparkSession.builder.appName("NewsProcessing")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# specify the schema for the incoming data
schema = StructType(
    [
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("text", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("url", StringType(), True),
        StructField("source", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("scraped_at", StringType(), True),
    ]
)

# set up spark to read from kafka news topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "news")
    .load()
    .selectExpr("CAST(value AS STRING) as json_string")  # cast the value as a string
    .select(from_json(col("json_string"), schema).alias("data"))  # select the data
    .select("data.*")
)


def get_sentiment(text: str) -> str:
    """
    Makes request to sentiment analysis API to get sentiment
    :param text: the text to get the sentiment of
    :return: the sentiment of the text
    """

    headers = {"Content-Type": "application/json"}
    r = requests.post('http://45.55.199.149:9000/api', json={'text': text},
                      headers=headers)  # make request to sentiment analysis API
    result = r.json()  # get the result

    # return the sentiment
    if result[0]['label'] == "LABEL_0":
        return "NEGATIVE"
    return "POSITIVE"


# create a udf to get the sentiment
get_sentiment_udf = udf(get_sentiment, StringType())

# apply the udf to the summary column and create a new column called sentiment
df = df.withColumn('sentiment', get_sentiment_udf(col('summary')))

query = df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
