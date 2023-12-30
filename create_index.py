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
