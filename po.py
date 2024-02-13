import json
import time
from kafka import KafkaProducer
import requests
from concurrent.futures import ThreadPoolExecutor
import threading

# This are the basic connection for the local running kafka server!
bootstrap_servers = 'localhost:9092'  
kafka_topic = 'weather'  

# It is used for kafka Producer and also to encrypt the incoming data !
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# This are the required connection details needed to work with Api ! end_point !
API_KEY = "70fb0a757c2bfa230e8f00cce4e6575b"
CITIES = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore']
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

exit_flag = False # This is used to close the loop and the connection @

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
            print("Weather data for {} pushed to Kafka !".format(city))
        else:
            print("Failed to fetch data for {}. Status code: {}".format(city, response.status_code))
    except requests.RequestException as e:
        print("Request Exception: {}".format(e))

def fetch_data_and_push(city): # Taking the data on all the listed city's !
    while not exit_flag:
        try:
            get_weather_data(city)
            time.sleep(50)
        except Exception as e:
            print("Error for city {}: {}".format(city, e))

def main(): # Using Threading !
    threads = []

    
    for city in CITIES:
        thread = threading.Thread(target=fetch_data_and_push, args=(city,))
        thread.start()
        threads.append(thread)

    try:
        
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Script interrupted by user.")
        global exit_flag
        exit_flag = True  

if __name__ == "__main__":
    main()
