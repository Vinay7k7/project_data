# import json
# import time
# from kafka import KafkaProducer
# import requests

# # Kafka producer configuration
# bootstrap_servers = 'your_kafka_bootstrap_servers'  # Replace with your Kafka bootstrap servers
# kafka_topic = 'your_kafka_topic'  # Replace with your Kafka topic

# # Create Kafka producer
# producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# # API endpoint to fetch data
# api_endpoint = 'https://jsonplaceholder.typicode.com/todos/1'

# def fetch_data_from_api():
#     try:
#         response = requests.get(api_endpoint)
#         data = response.json()
#         return data
#     except requests.RequestException as e:
#         print(f"Request Exception: {e}")
#         return None

# def push_to_kafka(data):
#     if data:
#         producer.send(kafka_topic, value=data)
#         print(f"Data pushed to Kafka: {data}")

# def main():
#     while True:
#         api_data = fetch_data_from_api()
#         push_to_kafka(api_data)
#         time.sleep(5)  # Sleep for 5 seconds before the next iteration

# if __name__ == "__main__":
#     main()




# import requests
# import time

# API_KEY = "70fb0a757c2bfa230e8f00cce4e6575b"
# CITIES = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore']
# BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# def get_weather_data(city):
#     params = {
#         'q': city,
#         'appid': API_KEY,
#         'units': 'metric'
#     }
#     try:
#         response = requests.get(BASE_URL, params=params)
#         data = response.json()
#         if response.status_code == 200:
#             print(data)
#         else:
#             print(f"Failed to fetch data for {city}. Status code: {response.status_code}")
#     except requests.RequestException as e:
#         print(f"Request Exception: {e}")

# def main():
#     while True:
#         for city in CITIES:
#             get_weather_data(city)
#             time.sleep(10)  # Sleep for 5 seconds between each city request

# if __name__ == "__main__":
#     main()








import json
import time
from kafka import KafkaProducer
import requests

# Kafka producer configuration
bootstrap_servers = 'your_kafka_bootstrap_servers'  # Replace with your Kafka bootstrap servers
kafka_topic = 'your_kafka_topic'  # Replace with your Kafka topic

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_KEY = "70fb0a757c2bfa230e8f00cce4e6575b"
CITIES = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore']
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def get_weather_data(city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    try:
        response = requests.get(BASE_URL, params=params)
        data = response.json()
        if response.status_code == 200:
            producer.send(kafka_topic, value=data)
            print(f"Weather data for {city} pushed to Kafka: {data}")
        else:
            print(f"Failed to fetch data for {city}. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Request Exception: {e}")

def main():
    while True:
        for city in CITIES:
            get_weather_data(city)
            time.sleep(10)  # Sleep for 10 seconds between each city request

if __name__ == "__main__":
    main()
