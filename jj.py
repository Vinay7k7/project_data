import os
from pyspark.sql import SparkSession

# Initialize findspark to locate Spark in the system
# import findspark
# findspark.init()

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create a DataFrame
data = [("John", 25), ("Alice", 30), ("Bob", 22)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()
