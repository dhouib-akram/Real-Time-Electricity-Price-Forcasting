from kafka import KafkaProducer
import argparse
import csv
import time
import json

def read_csv(file_path):
    data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data
weather_schema = {
    "dt_iso": str,
    "city_name": str,
    "temp": float,
    "temp_min": float,
    "temp_max": float,
    "pressure": int,
    "humidity": int,
    "wind_speed": float,
    "wind_deg": int,
    "rain_1h": float,
    "rain_3h": float,
    "snow_3h": float,
    "clouds_all": int,
    "weather_id": int,
    "weather_main": str,
    "weather_description": str,
    "weather_icon": str
}
energy_schema = {
    "time": str,
    "generation biomass": float,
    "generation fossil brown coal/lignite": float,
    "generation fossil coal-derived gas": float,
    "generation fossil gas": float,
    "generation fossil hard coal": float,
    "generation fossil oil": float,
    "generation fossil oil shale": float,
    "generation fossil peat": float,
    "generation geothermal": float,
    "generation hydro pumped storage aggregated": float,
    "generation hydro pumped storage consumption": float,
    "generation hydro run-of-river and poundage": float,
    "generation hydro water reservoir": float,
    "generation marine": float,
    "generation nuclear": float,
    "generation other": float,
    "generation other renewable": float,
    "generation solar": float,
    "generation waste": float,
    "generation wind offshore": float,
    "generation wind onshore": float,
    "forecast solar day ahead": float,
    "forecast wind offshore eday ahead": float,
    "forecast wind onshore day ahead": float,
    "total load forecast": float,
    "total load actual": float,
    "price day ahead": float,
    "price actual": float
}

if __name__ == "__main__":
    bootstrap_servers = 'localhost:9093'

    # For weather data
    weather_data_path = "./data/weather_features.csv"
    weather_topic = "weather"
    weather_data = read_csv(weather_data_path)
   
    # For energy data
    energy_data_path = "./data/energy_dataset.csv"
    energy_topic = "energy"
    energy_data = read_csv(energy_data_path)
   
    # Sort weather data by timestamp
    weather_data = sorted(weather_data, key=lambda x: (x['dt_iso'], x['city_name']))

    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Send data to both Kafka topics simultaneously
    for record_weather, record_energy in zip(weather_data, energy_data):
        # Send 5 rows for the same timestamp but different cities for weather data
        for city_record in weather_data:
            if city_record['dt_iso'] == record_weather['dt_iso']:
                casted_city_record = {key: weather_schema[key](city_record[key]) for key in weather_schema}
                producer.send(weather_topic, value=casted_city_record)
                print(f"Sent message to {weather_topic}: {casted_city_record}")
       
        # Send energy data for the same timestamp
        casted_record_energy = {key: energy_schema[key](record_energy[key]) if record_energy[key] != '' else None for key in energy_schema}
        casted_record_energy_serializable = {key: value if value is None or isinstance(value, (int, float, str, bool, list, dict)) else str(value) for key, value in casted_record_energy.items()}
        producer.send(energy_topic, value=casted_record_energy_serializable)
        print(f"Sent message to {energy_topic}: {casted_record_energy_serializable}")
       
        print("___________________________")
        time.sleep(10)

    # Flush and close the producer
    producer.flush()
    producer.close()