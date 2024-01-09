from elasticsearch import Elasticsearch



# Create an Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Define the index name

def create_index(index_name,mapping):
    if es.indices.exists(index=index_name):
        # Delete the index
        print(f"Index '{index_name}' exists.")
        es.indices.delete(index=index_name)
        print(f"Index '{index_name}' deleted.")
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created.")
    



# Define the mapping for the "weather" index
weather_mapping = {
    "mappings": {
        "properties": {
            "city_name": {"type": "keyword"},
            "temp": {"type": "double"},
            "temp_min": {"type": "double"},
            "temp_max": {"type": "double"},
            "pressure": {"type": "integer"},
            "humidity": {"type": "integer"},
            "wind_speed": {"type": "double"},
            "wind_deg": {"type": "integer"},
            "snow_3h": {"type": "double"},
            "clouds_all": {"type": "integer"},
            "timestamp": {"type": "date"},
            
        }
    }
}
# Create the "weather" index with the specified mapping

create_index("weather",weather_mapping)


# Define the mapping for the "energy" index
energy_mapping = {
    "mappings": {
        "properties": {
            "generation biomass": {"type": "double"},
            "generation fossil brown coal/lignite": {"type": "double"},
            "generation fossil gas": {"type": "double"},
            "generation fossil hard coal": {"type": "double"},
            "generation fossil oil": {"type": "double"},
            "generation hydro run-of-river and poundage": {"type": "double"},
            "generation hydro water reservoir": {"type": "double"},
            "generation nuclear": {"type": "double"},
            "generation other": {"type": "double"},
            "generation other renewable": {"type": "double"},
            "generation solar": {"type": "double"},
            "generation waste": {"type": "double"},
            "generation wind onshore": {"type": "double"},
            "total load actual": {"type": "double"},
            "price actual": {"type": "double"},
            "timestamp": {"type": "date"},
        }
    }
}

# Create the "energy" index with the specified mapping
create_index("energy",energy_mapping)

# Elasticsearch mapping for the 'merged' index
# merged_mapping = {
#     "mappings": {
#         "properties": {
#             "generation biomass": {"type": "double"},
#             "generation fossil brown coal/lignite": {"type": "double"},
#             "generation fossil gas": {"type": "double"},
#             "generation fossil hard coal": {"type": "double"},
#             "generation fossil oil": {"type": "double"},
#             "generation hydro pumped storage consumption": {"type": "double"},
#             "generation hydro run-of-river and poundage": {"type": "double"},
#             "generation hydro water reservoir": {"type": "double"},
#             "generation nuclear": {"type": "double"},
#             "generation other": {"type": "double"},
#             "generation other renewable": {"type": "double"},
#             "generation solar": {"type": "double"},
#             "generation waste": {"type": "double"},
#             "generation wind onshore": {"type": "double"},
#             "total load actual": {"type": "double"},
#             "temp_Barcelona": {"type": "double"},
#             "temp_min_Barcelona": {"type": "double"},
#             "temp_max_Barcelona": {"type": "double"},
#             "pressure_Barcelona": {"type": "integer"},
#             "humidity_Barcelona": {"type": "integer"},
#             "wind_speed_Barcelona": {"type": "double"},
#             "wind_deg_Barcelona": {"type": "integer"},
#             "rain_1h_Barcelona": {"type": "double"},
#             "clouds_all_Barcelona": {"type": "integer"},
#             "temp_Bilbao": {"type": "double"},
#             "temp_min_Bilbao": {"type": "double"},
#             "temp_max_Bilbao": {"type": "double"},
#             "pressure_Bilbao": {"type": "integer"},
#             "humidity_Bilbao": {"type": "integer"},
#             "wind_speed_Bilbao": {"type": "double"},
#             "wind_deg_Bilbao": {"type": "integer"},
#             "rain_1h_Bilbao": {"type": "double"},
#             "snow_3h_Bilbao": {"type": "double"},
#             "clouds_all_Bilbao": {"type": "integer"},
#             "temp_Madrid": {"type": "double"},
#             "temp_min_Madrid": {"type": "double"},
#             "temp_max_Madrid": {"type": "double"},
#             "pressure_Madrid": {"type": "integer"},
#             "humidity_Madrid": {"type": "integer"},
#             "wind_speed_Madrid": {"type": "double"},
#             "wind_deg_Madrid": {"type": "integer"},
#             "rain_1h_Madrid": {"type": "double"},
#             "snow_3h_Madrid": {"type": "double"},
#             "clouds_all_Madrid": {"type": "integer"},
#             "temp_Seville": {"type": "double"},
#             "temp_min_Seville": {"type": "double"},
#             "temp_max_Seville": {"type": "double"},
#             "pressure_Seville": {"type": "integer"},
#             "humidity_Seville": {"type": "integer"},
#             "wind_speed_Seville": {"type": "double"},
#             "wind_deg_Seville": {"type": "integer"},
#             "rain_1h_Seville": {"type": "double"},
#             "clouds_all_Seville": {"type": "integer"},
#             "temp_Valencia": {"type": "double"},
#             "temp_min_Valencia": {"type": "double"},
#             "temp_max_Valencia": {"type": "double"},
#             "pressure_Valencia": {"type": "integer"},
#             "humidity_Valencia": {"type": "integer"},
#             "wind_speed_Valencia": {"type": "double"},
#             "wind_deg_Valencia": {"type": "integer"},
#             "rain_1h_Valencia": {"type": "double"},
#             "snow_3h_Valencia": {"type": "double"},
#             "clouds_all_Valencia": {"type": "integer"},
#             "hour": {"type": "integer"},
#             "weekday": {"type": "integer"},
#             "month": {"type": "integer"},
#             "business_hour": {"type": "integer"},
#             "temp_range_Barcelona": {"type": "double"},
#             "temp_range_Bilbao": {"type": "double"},
#             "temp_range_Madrid": {"type": "double"},
#             "temp_range_Seville": {"type": "double"},
#             "temp_range_Valencia": {"type": "double"},
#             "temp_weighted": {"type": "double"},
#             "generation_coal_all": {"type": "double"},
#             "day": {"type": "integer"},
#             "year": {"type": "integer"},
#             "timestamp": {"type": "date"}
#         }
#     }
# }

prediction_mapping = {
    "mappings": {
        "properties": {

            "predicted_price": {"type": "double"},
            "timestamp": {"type": "date"}
        }
    }
}
create_index("prediction",prediction_mapping)
