import json
import time
from kafka import KafkaProducer
import requests
from concurrent.futures import ThreadPoolExecutor
import threading

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
            print("Weather data for {} pushed to Kafka: {}".format(city, data))
        else:
            print("Failed to fetch data for {}. Status code: {}".format(city, response.status_code))
    except requests.RequestException as e:
        print("Request Exception: {}".format(e))

def fetch_data_and_push(city):
    while not exit_flag:
        try:
            get_weather_data(city)
            time.sleep(10)  # Sleep for 10 seconds before fetching data for the same city again
        except Exception as e:
            print("Error for city {}: {}".format(city, e))

def main():
    threads = []

    # Create and start a thread for each city
    for city in CITIES:
        thread = threading.Thread(target=fetch_data_and_push, args=(city,))
        thread.start()
        threads.append(thread)

    try:
        # Keep the main thread alive while the city threads are running
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Script interrupted by user.")
        global exit_flag
        exit_flag = True  # Set the exit flag to True

if __name__ == "__main__":
    main()
