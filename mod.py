import json
import time
from kafka import KafkaProducer
import requests
from concurrent.futures import ThreadPoolExecutor

# Kafka producer configuration
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka bootstrap servers
kafka_topic = 'weather-topic'  # Replace with your Kafka topic

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_KEY = "70fb0a757c2bfa230e8f00cce4e6575b"
CITIES = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore']
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

exit_flag = False  # Variable to control the loop

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
            print("Weather data for {} pushed to Kafka: {}".format(city,data))
        else:
            print("Failed to fetch data for {}. Status code: {}".format(city,response.status_code))
    except requests.RequestException as e:
        print("Request Exception: {}".format(e))

def fetch_data_and_push(city):
    try:
        get_weather_data(city)
    except Exception as e:
        print("Error for city {}: {}".format(city,e))

def main():
    with ThreadPoolExecutor(max_workers=len(CITIES)) as executor:
        try:
            executor.map(fetch_data_and_push, CITIES)
        except KeyboardInterrupt:
            print("Script interrupted by user.")
            global exit_flag
            exit_flag = True  # Set the exit flag to True

if __name__ == "__main__":
    main()
