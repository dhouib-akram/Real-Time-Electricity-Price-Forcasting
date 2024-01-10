from kafka import KafkaConsumer
import json
import pandas as pd
import pickle
import tensorflow as tf
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])



# Kafka consumer configuration
consumer_conf = {
    'bootstrap_servers': 'localhost:9093',
    'auto_offset_reset': 'earliest',
}

# Load scaler, PCA, and the model from pickle and h5 files
with open('./data/model-preprocess/scaler_X.pkl', 'rb') as f:
    scaler_X = pickle.load(f)

with open('./data/model-preprocess/pca.pkl', 'rb') as f:
    pca = pickle.load(f)

with open('./data/model-preprocess/scaler_y.pkl', 'rb') as f:
    scaler_y = pickle.load(f)

# Load the LSTM model
multivariate_lstm = tf.keras.models.load_model('./data/model-preprocess/multivariate_lstm.h5')

# Create Kafka consumer
consumer = KafkaConsumer('merged', **consumer_conf)

# Initialize a buffer for the sliding window
window_buffer = []
# Initialize a list for storing DataFrames
dataframes_list = []

predicted_value = 0  # Initialize the predicted value

columns_order=['generation biomass', 'generation fossil brown coal/lignite',
       'generation fossil gas', 'generation fossil hard coal',
       'generation fossil oil', 'generation hydro pumped storage consumption',
       'generation hydro run-of-river and poundage',
       'generation hydro water reservoir', 'generation nuclear',
       'generation other', 'generation other renewable', 'generation solar',
       'generation waste', 'generation wind onshore', 'total load actual',
       'temp_Barcelona', 'temp_min_Barcelona', 'temp_max_Barcelona',
       'pressure_Barcelona', 'humidity_Barcelona', 'wind_speed_Barcelona',
       'wind_deg_Barcelona', 'rain_1h_Barcelona', 'clouds_all_Barcelona',
       'temp_Bilbao', 'temp_min_Bilbao', 'temp_max_Bilbao', 'pressure_Bilbao',
       'humidity_Bilbao', 'wind_speed_Bilbao', 'wind_deg_Bilbao',
       'rain_1h_Bilbao', 'snow_3h_Bilbao', 'clouds_all_Bilbao', 'temp_Madrid',
       'temp_min_Madrid', 'temp_max_Madrid', 'pressure_Madrid',
       'humidity_Madrid', 'wind_speed_Madrid', 'wind_deg_Madrid',
       'rain_1h_Madrid', 'snow_3h_Madrid', 'clouds_all_Madrid', 'temp_Seville',
       'temp_min_Seville', 'temp_max_Seville', 'pressure_Seville',
       'humidity_Seville', 'wind_speed_Seville', 'wind_deg_Seville',
       'rain_1h_Seville', 'clouds_all_Seville', 'temp_Valencia',
       'temp_min_Valencia', 'temp_max_Valencia', 'pressure_Valencia',
       'humidity_Valencia', 'wind_speed_Valencia', 'wind_deg_Valencia',
       'rain_1h_Valencia', 'snow_3h_Valencia', 'clouds_all_Valencia', 'hour',
       'weekday', 'month', 'business_hour', 'temp_range_Barcelona',
       'temp_range_Bilbao', 'temp_range_Madrid', 'temp_range_Seville',
       'temp_range_Valencia', 'temp_weighted', 'generation_coal_all', 'day',
       'year']

savedf=True
index=0
for message in consumer:
    try:
        # Decode and parse the message
        message_value = message.value.decode('utf-8')
        json_data = json.loads(message_value)
        # Add predicted price to the json data
        json_data['predicted_price'] = predicted_value
        index+=1
        print('index:',index)
        # Index the document into Elasticsearch
        es_document = {
            'predicted_price': predicted_value,
            'timestamp': json_data['timestamp']
        }
        es.index(index='prediction', document=es_document)
        # Convert JSON data to DataFrame and preprocess
        df = pd.DataFrame([json_data])

        df_reordered = df[columns_order]


        y_value= df['price actual'].values
        # If predicted value from the previous iteration exists, display it with the actual value
        if predicted_value is not None:
            print(f"Actual value: {y_value}")
            print(f"Predicted value from previous hour: {predicted_value}")
        
        X = df_reordered.values
        X_scaled = scaler_X.transform(X)
        X_pca = pca.transform(X_scaled)
        y = y_value.reshape(-1, 1)
        y_norm = scaler_y.transform(y)
        dataset_norm = np.concatenate((X_pca, y_norm), axis=1)

        # Add to buffer
        window_buffer.append(dataset_norm)
        # Check if buffer has 24 messages
        if len(window_buffer) == 24:
            # Convert buffer to array and reshape for LSTM
            X_pca_buffer = np.array(window_buffer).reshape((1, 24, -1))


            # Predict
            prediction = multivariate_lstm.predict(X_pca_buffer)
            predicted_value = scaler_y.inverse_transform(prediction)
            # Slide the window: remove first (oldest) message
            window_buffer.pop(0)
    except KeyError as ke:
        print(f"KeyError occurred: {ke}")
        print("Skipping to the next message.")
        continue
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

# Close the Kafka consumer
consumer.close()
