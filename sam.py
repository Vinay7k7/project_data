from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Define the schema for the incoming data
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", FloatType()),
        StructField("lat", FloatType())
    ])),
    StructField("weather", StringType()),
    StructField("base", StringType()),
    # ... (the rest of your schema)
])

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaStreamProcessing").getOrCreate()

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "weather-topic"
}

# Create a DataFrame that represents streaming data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
    .option("subscribe", kafka_params["subscribe"]) \
    .load()

# Extract the value field from Kafka message and convert it to JSON
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data"))

# Add a timestamp column
timestamped_df = json_df.withColumn("timestamp", current_timestamp())

# Flatten the nested structure to make it easier to work with
flattened_df = timestamped_df.select("timestamp", "data.*")

# Perform your processing here, for example, you can write the data to the console
query = flattened_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Start the query
query.awaitTermination()
