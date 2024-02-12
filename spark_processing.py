from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder.appName("NewsProcessing")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

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

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "news")
    .load()
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), schema).alias("data"))
    .select("data.*")
)

query = df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
