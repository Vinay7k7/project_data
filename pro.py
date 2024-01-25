# # import requests
# # import time

# # API_KEY = "70fb0a757c2bfa230e8f00cce4e6575b"
# # CITIES = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore']
# # BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# # def get_weather_data(city):
# #     params = {
# #         'q': city,
# #         'appid': API_KEY,
# #         'units': 'metric'
# #     }
# #     try:
# #         response = requests.get(BASE_URL, params=params)
# #         data = response.json()
# #         if response.status_code == 200:
# #             print(data)
# #         else:
# #             print(f"Failed to fetch data for {city}. Status code: {response.status_code}")
# #     except requests.RequestException as e:
# #         print(f"Request Exception: {e}")

# # def main():
# #     while True:
# #         for city in CITIES:
# #             get_weather_data(city)
# #             time.sleep(10)  # Sleep for 5 seconds between each city request

# # if __name__ == "__main__":
# #     main()







# # If u want to view the data in the local system cmd then first you need to run the zookeeper server for that ! (In new terminal !)

# # ----->bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# # After that you need to start the kafka server also for that you need to use ! (In new terminal !)

# # ------>bin\windows\kafka-server-start.bat config\server.properties

# #After that you need to view the data from the topic for that you need to use ! (In new terminal !)

# # ----->bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic weather-data --from-beginning
# # and remember ti replace the topic name before you run this ! commend in the cmd 


# import json
# import time
# from confluent_kafka import Producer #Used for kafka connection !
# import requests

# API_KEY = "70fb0a757c2bfa230e8f00cce4e6575b"
# CITIES = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore']
# BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# # Kafka producer configuration
# bootstrap_servers = 'localhost:9092'  
# topic = 'weather-data' 

# conf = {
#     'bootstrap.servers': bootstrap_servers,
# }

# # Create Kafka producer
# producer = Producer(conf)


# class WeatherInfo:
#     def __init__(self, city_name, lon, lat, wind_speed, max_temp, min_temp, current_temp, pressure, humidity, timezone, timestamp):
#         self.city_name = city_name
#         self.lon = lon
#         self.lat = lat
#         self.wind_speed = wind_speed
#         self.max_temp = max_temp
#         self.min_temp = min_temp
#         self.current_temp = current_temp
#         self.pressure = pressure
#         self.humidity = humidity
#         self.timezone = timezone
#         self.timestamp = timestamp

#     def to_json(self):
#         return json.dumps({
#             'City': self.city_name,
#             'Coordinates': {'Longitude': self.lon, 'Latitude': self.lat},
#             'Wind Speed': self.wind_speed,
#             'Max Temperature': self.max_temp,
#             'Min Temperature': self.min_temp,
#             'Current Temperature': self.current_temp,
#             'Pressure': self.pressure,
#             'Humidity': self.humidity,
#             'Timezone': self.timezone,
#             'Timestamp': self.timestamp
#         })


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
#             city_name = data['name']
#             lon = data['coord']['lon']
#             lat = data['coord']['lat']
#             wind_speed = data['wind']['speed']
#             max_temp = data['main']['temp_max']
#             min_temp = data['main']['temp_min']
#             current_temp = data['main']['temp']
#             pressure = data['main']['pressure']
#             humidity = data['main']['humidity']
#             timezone = data['timezone']
#             timestamp = int(time.time())

#             weather_info = WeatherInfo(city_name, lon, lat, wind_speed, max_temp, min_temp, current_temp, pressure, humidity, timezone, timestamp)
#             message = weather_info.to_json()
#             producer.produce('weather-topic', message.encode('utf-8'), callback=delivery_report)
#             producer.flush()

#         else:
#             print(f"Failed to fetch data for {city}. Status code: {response.status_code}")
#     except requests.RequestException as e:
#         print(f"Request Exception: {e}")


# def delivery_report(err, msg):
#     if err is not None:
#         print(f'Message delivery failed: {err}')
#     else:
#         print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# def main():
#     while True:
#         for city in CITIES:
#             get_weather_data(city)
#             time.sleep(5)  # Sleep for 5 seconds between each city request


# if __name__ == "__main__":
#     main()

# # def delivery_report(err, msg):
# #     if err is not None:
# #         print('Message delivery failed: {}'.format(err))
# #     else:
# #         print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# # def get_weather_data(city):
# #     params = {
# #         'q': city,
# #         'appid': API_KEY,
# #         'units': 'metric'
# #     }
# #     try:
# #         response = requests.get(BASE_URL, params=params)
# #         data = response.json()
# #         if response.status_code == 200:
# #             # Produce the weather data to the Kafka topic
# #             message_value = json.dumps(data)
# #             producer.produce(topic, key=city, value=message_value, callback=delivery_report)
# #             producer.flush()
# #             print(f"Produced weather data for {city}")
# #         else:
# #             print(f"Failed to fetch data for {city}. Status code: {response.status_code}")
# #     except requests.RequestException as e:
# #         print(f"Request Exception: {e}")

# # def main():
# #     while True:
# #         for city in CITIES:
# #             get_weather_data(city)
# #             time.sleep(5)  # Sleep for 5 seconds between each city request

# # if __name__ == "__main__":
# #     main()
import json
import time
import asyncio
from confluent_kafka import Producer
import aiohttp

API_KEY = "70fb0a757c2bfa230e8f00cce4e6575b"
CITIES = ['Hyderabad', 'Delhi', 'Chennai', 'Kolkata', 'Bangalore']
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'weather-producer'
}

producer = Producer(producer_conf)


class WeatherInfo:
    def __init__(self, city_name, lon, lat, wind_speed, max_temp, min_temp, current_temp, pressure, humidity, timezone,
                timestamp):
        self.city_name = city_name
        self.lon = lon
        self.lat = lat
        self.wind_speed = wind_speed
        self.max_temp = max_temp
        self.min_temp = min_temp
        self.current_temp = current_temp
        self.pressure = pressure
        self.humidity = humidity
        self.timezone = timezone
        self.timestamp = timestamp

    def to_json(self):
        return json.dumps({
            'City': self.city_name,
            'Coordinates': {'Longitude': self.lon, 'Latitude': self.lat},
            'Wind Speed': self.wind_speed,
            'Max Temperature': self.max_temp,
            'Min Temperature': self.min_temp,
            'Current Temperature': self.current_temp,
            'Pressure': self.pressure,
            'Humidity': self.humidity,
            'Timezone': self.timezone,
            'Timestamp': self.timestamp
        })


async def fetch_weather_data(session, city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    try:
        async with session.get(BASE_URL, params=params) as response:
            data = await response.json()
            if response.status == 200:
                city_name = data['name']
                lon = data['coord']['lon']
                lat = data['coord']['lat']
                wind_speed = data['wind']['speed']
                max_temp = data['main']['temp_max']
                min_temp = data['main']['temp_min']
                current_temp = data['main']['temp']
                pressure = data['main']['pressure']
                humidity = data['main']['humidity']
                timezone = data['timezone']
                timestamp = int(time.time())

                weather_info = WeatherInfo(city_name, lon, lat, wind_speed, max_temp, min_temp, current_temp,
                                        pressure, humidity, timezone, timestamp)
                message = weather_info.to_json()
                producer.produce('weather-topic', message.encode('utf-8'), callback=delivery_report)
                producer.flush()

            else:
                print(f"Failed to fetch data for {city}. Status code: {response.status}")
    except aiohttp.ClientError as e:
        print(f"Client error for {city}: {e}")


async def main():
    while True:
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_weather_data(session, city) for city in CITIES]
            await asyncio.gather(*tasks)
        await asyncio.sleep(60)  # Sleep for 30 seconds before the next iteration


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if __name__ == "__main__":
    asyncio.run(main())
