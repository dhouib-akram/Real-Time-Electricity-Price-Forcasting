from kafka import KafkaConsumer
import json
import pandas as pd
import pickle
import tensorflow as tf

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

for message in consumer:
    try:
        # Decode the message value
        message_value = message.value.decode('utf-8')
        # Parse the JSON data
        json_data = json.loads(message_value)
        # Convert JSON data to DataFrame
        df = pd.DataFrame([json_data])

        # Separate features and target
        X = df.drop(columns=['price actual', "timestamp"])
        y = df['price actual']

        # Apply scaler and PCA transformations
        X_scaled = scaler_X.transform(X)
        X_pca = pca.transform(X_scaled)

        # Predict using the LSTM model
        prediction = multivariate_lstm.predict(X_pca)
        prediction_inversed = scaler_y.inverse_transform(prediction)

        # Print the transformed features, predicted, and actual values
        print(f"Transformed features: {X_pca.tolist()}")
        print('-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+')
        print(f"Predicted value: {prediction_inversed[0][0]}")
        print(f"Actual value: {y.values[0]}")
        print('-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+')

    except json.JSONDecodeError as e:
        # Handle the JSONDecodeError
        print(f"Error decoding JSON: {e}")

# Close the Kafka consumer
consumer.close()
