from pymongo import MongoClient

# Replace this with your MongoDB Atlas connection string
connection_string = "your_connection_string"

# Create a MongoClient instance
client = MongoClient(connection_string)

# Access the database (replace 'your_database' with the actual database name)
db = client.your_database

# Access the collection (replace 'your_collection' with the actual collection name)
collection = db.your_collection

# Data to be inserted
data_to_insert = {
    "key1": "value1",
    "key2": "value2",
    # Add more fields as needed
}

# Insert data into the collection
result = collection.insert_one(data_to_insert)

# Print the inserted document's ID
print("Inserted document ID:", result.inserted_id)

# Close the MongoDB connection
client.close()
