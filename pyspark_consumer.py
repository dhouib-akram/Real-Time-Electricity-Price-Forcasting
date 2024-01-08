

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
from pyspark.sql.functions import col, when, dayofweek, hour, dayofmonth, month, year, broadcast
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import joblib
from pyspark.sql.functions import to_json, struct



# Total population for weight calculation
total_pop = 6155116 + 5179243 + 1645342 + 1305342 + 987000

# Define weights for each city
weights = {
    'Madrid': 6155116 / total_pop,
    'Barcelona': 5179243 / total_pop,
    'Valencia': 1645342 / total_pop,
    'Seville': 1305342 / total_pop,
    'Bilbao': 987000 / total_pop
}

es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

spark = SparkSession.builder.appName("electr") \
    .config("spark.default.parallelism", 4) \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the schema for weather data
weather_schema = StructType().add("dt_iso", StringType()) \
    .add("temp_Barcelona", DoubleType()) \
    .add("temp_min_Barcelona", DoubleType()) \
    .add("temp_max_Barcelona", DoubleType()) \
    .add("pressure_Barcelona", IntegerType()) \
    .add("humidity_Barcelona", IntegerType()) \
    .add("wind_speed_Barcelona", DoubleType()) \
    .add("wind_deg_Barcelona", IntegerType()) \
    .add("rain_1h_Barcelona", DoubleType()) \
    .add("snow_3h_Barcelona", DoubleType()) \
    .add("clouds_all_Barcelona", IntegerType()) \
    .add("weather_id_Barcelona", IntegerType()) \
    .add("weather_main_Barcelona", StringType()) \
    .add("weather_description_Barcelona", StringType()) \
    .add("weather_icon_Barcelona", StringType()) \
    .add("temp_Bilbao", DoubleType()) \
    .add("temp_min_Bilbao", DoubleType()) \
    .add("temp_max_Bilbao", DoubleType()) \
    .add("pressure_Bilbao", IntegerType()) \
    .add("humidity_Bilbao", IntegerType()) \
    .add("wind_speed_Bilbao", DoubleType()) \
    .add("wind_deg_Bilbao", IntegerType()) \
    .add("rain_1h_Bilbao", DoubleType()) \
    .add("snow_3h_Bilbao", DoubleType()) \
    .add("clouds_all_Bilbao", IntegerType()) \
    .add("weather_id_Bilbao", IntegerType()) \
    .add("weather_main_Bilbao", StringType()) \
    .add("weather_description_Bilbao", StringType()) \
    .add("weather_icon_Bilbao", StringType()) \
    .add("temp_Madrid", DoubleType()) \
    .add("temp_min_Madrid", DoubleType()) \
    .add("temp_max_Madrid", DoubleType()) \
    .add("pressure_Madrid", IntegerType()) \
    .add("humidity_Madrid", IntegerType()) \
    .add("wind_speed_Madrid", DoubleType()) \
    .add("wind_deg_Madrid", IntegerType()) \
    .add("rain_1h_Madrid", DoubleType()) \
    .add("snow_3h_Madrid", DoubleType()) \
    .add("clouds_all_Madrid", IntegerType()) \
    .add("weather_id_Madrid", IntegerType()) \
    .add("weather_main_Madrid", StringType()) \
    .add("weather_description_Madrid", StringType()) \
    .add("weather_icon_Madrid", StringType()) \
    .add("temp_Seville", DoubleType()) \
    .add("temp_min_Seville", DoubleType()) \
    .add("temp_max_Seville", DoubleType()) \
    .add("pressure_Seville", IntegerType()) \
    .add("humidity_Seville", IntegerType()) \
    .add("wind_speed_Seville", DoubleType()) \
    .add("wind_deg_Seville", IntegerType()) \
    .add("rain_1h_Seville", DoubleType()) \
    .add("snow_3h_Seville", DoubleType()) \
    .add("clouds_all_Seville", IntegerType()) \
    .add("weather_id_Seville", IntegerType()) \
    .add("weather_main_Seville", StringType()) \
    .add("weather_description_Seville", StringType()) \
    .add("weather_icon_Seville", StringType()) \
    .add("temp_Valencia", DoubleType()) \
    .add("temp_min_Valencia", DoubleType()) \
    .add("temp_max_Valencia", DoubleType()) \
    .add("pressure_Valencia", IntegerType()) \
    .add("humidity_Valencia", IntegerType()) \
    .add("wind_speed_Valencia", DoubleType()) \
    .add("wind_deg_Valencia", IntegerType()) \
    .add("rain_1h_Valencia", DoubleType()) \
    .add("snow_3h_Valencia", DoubleType()) \
    .add("clouds_all_Valencia", IntegerType()) \
    .add("weather_id_Valencia", IntegerType()) \
    .add("weather_main_Valencia", StringType()) \
    .add("weather_description_Valencia", StringType()) \
    .add("weather_icon_Valencia", StringType())

# Define the schema for energy data
energy_schema = StructType().add("time", StringType()) \
    .add("generation biomass", DoubleType()) \
    .add("generation fossil brown coal/lignite", DoubleType()) \
    .add("generation fossil coal-derived gas", DoubleType()) \
    .add("generation fossil gas", DoubleType()) \
    .add("generation fossil hard coal", DoubleType()) \
    .add("generation fossil oil", DoubleType()) \
    .add("generation fossil oil shale", DoubleType()) \
    .add("generation fossil peat", DoubleType()) \
    .add("generation geothermal", DoubleType()) \
    .add("generation hydro pumped storage aggregated", DoubleType()) \
    .add("generation hydro pumped storage consumption", DoubleType()) \
    .add("generation hydro run-of-river and poundage", DoubleType()) \
    .add("generation hydro water reservoir", DoubleType()) \
    .add("generation marine", DoubleType()) \
    .add("generation nuclear", DoubleType()) \
    .add("generation other", DoubleType()) \
    .add("generation other renewable", DoubleType()) \
    .add("generation solar", DoubleType()) \
    .add("generation waste", DoubleType()) \
    .add("generation wind offshore", DoubleType()) \
    .add("generation wind onshore", DoubleType()) \
    .add("forecast solar day ahead", DoubleType()) \
    .add("forecast wind offshore eday ahead", DoubleType()) \
    .add("forecast wind onshore day ahead", DoubleType()) \
    .add("total load actual", DoubleType()) \
    .add("price actual", DoubleType())

# Energy data processing
energy_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "energy")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), energy_schema).alias("energy_data"))
    .select("energy_data.*")
    .withColumn("timestamp", col("time").cast(TimestampType()))
    .drop("time")
    .drop(*['generation fossil coal-derived gas', 'generation fossil oil shale',
            'generation fossil peat', 'generation geothermal',
            'generation hydro pumped storage aggregated', 'generation marine',
            'generation wind offshore', 'forecast wind offshore eday ahead',
            'total load forecast', 'forecast solar day ahead',
            'forecast wind onshore day ahead'])
    .dropDuplicates(["timestamp"])
)
energy_df = energy_df.withColumn('generation_coal_all', col("generation fossil hard coal") + col("generation fossil brown coal/lignite"))

# Weather data processing
weather_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), weather_schema).alias("weather_data"))
    .select("weather_data.*")
    .withColumn("timestamp", col("dt_iso").cast(TimestampType()))
    .drop("dt_iso")
    .withWatermark("timestamp", "1 hour")
    .dropDuplicates(["timestamp"])
)

# Weather data cleanup
city_names = ["Barcelona", "Bilbao", "Madrid", "Seville", "Valencia"]

for city_name in city_names:
    weather_df = (
        weather_df
        .withColumn(f"pressure_{city_name}",
                    when((col(f"pressure_{city_name}") > 1051) | (col(f"pressure_{city_name}") < 931), None)
                    .otherwise(col(f"pressure_{city_name}")))
        .withColumn(f"wind_speed_{city_name}",
                    when((col(f"wind_speed_{city_name}") > 50), None).otherwise(col(f"wind_speed_{city_name}")))
    )

# Weather data feature engineering
deduplicated_weather_stream = (
    weather_df
    .withColumn("day", dayofmonth("timestamp"))
    .withColumn("month", month("timestamp"))
    .withColumn("year", year("timestamp"))
    .withColumn("weekday", dayofweek("timestamp"))
    .withColumn("hour", hour("timestamp"))
    .drop(*[f"weather_main_{city}" for city in city_names])
    .drop(*[f"weather_id_{city}" for city in city_names])
    .drop(*[f"weather_description_{city}" for city in city_names])
    .drop(*[f"weather_icon_{city}" for city in city_names])
    .withColumn(
        "business_hour",
        when(((col("hour") > 8) & (col("hour") < 14)) | ((col("hour") > 16) & (col("hour") < 21)), 2)
        .when((col("hour") >= 14) & (col("hour") <= 16), 1)
        .otherwise(0)
    )
    .withColumn(
        "weekday",
        when(dayofweek(col("timestamp")) == 6, 2)
        .when(dayofweek(col("timestamp")) == 5, 1)
        .otherwise(0)
    )
)

# Additional feature engineering for temperature range
for city in city_names:
    temp_max_col = col(f'temp_max_{city}')
    temp_min_col = col(f'temp_min_{city}')
    deduplicated_weather_stream = deduplicated_weather_stream.withColumn(f'temp_range_{city}', abs(temp_max_col - temp_min_col))
temp_weighted_expr = "+".join([f"temp_{city} * {weights.get(city)}" for city in city_names])
deduplicated_weather_stream = deduplicated_weather_stream.withColumn("temp_weighted", expr(temp_weighted_expr))
# Joining weather and energy data
joined_stream = (
    deduplicated_weather_stream.join(
        energy_df,
        (deduplicated_weather_stream.timestamp == energy_df.timestamp),
        "inner"
    )
    .drop(energy_df["timestamp"])
)


columns_to_drop = ['snow_3h_Barcelona', 'snow_3h_Seville']
joined_stream = joined_stream.drop(*columns_to_drop)

# scaled_joined_stream = joined_stream.select(scale_data(col("*")).alias("scaled_data"))

# # Writing the joined stream to the console (for testing purposes)
# query = (
#     joined_stream
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .option("numRows", 1000000)
#     .start()
# )
# query.awaitTermination()

# Define Kafka sink options
kafka_sink_options = {
    "kafka.bootstrap.servers": "kafka:9092",
    "topic": "merged",
    "value.serializer": "org.apache.kafka.common.serialization.StringSerializer"
}

# Write the transformed DataFrame to Kafka as a sink
def foreach_batch_function(df, epoch_id):
    print("Batch ID: ", epoch_id)
    df.show(truncate=False)
    df.select(to_json(struct("*")).alias("value")) \
      .write \
      .format("kafka") \
      .options(**kafka_sink_options) \
      .save()

query = (
    joined_stream
    .writeStream
    .outputMode("append")
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", "./dir")
    .start()
)

query.awaitTermination()






# scaler_X = joblib.load('scaler_X.pkl')

# # List of columns to be scaled
# scaler_columns = [
#     "temp_Barcelona", "temp_min_Barcelona", "temp_max_Barcelona",
#     "pressure_Barcelona", "humidity_Barcelona", "wind_speed_Barcelona",
#     "wind_deg_Barcelona", "rain_1h_Barcelona", "snow_3h_Barcelona",
#     "clouds_all_Barcelona", "temp_Bilbao", "temp_min_Bilbao", "temp_max_Bilbao",
#     "pressure_Bilbao", "humidity_Bilbao", "wind_speed_Bilbao",
#     "wind_deg_Bilbao", "rain_1h_Bilbao", "snow_3h_Bilbao", "clouds_all_Bilbao",
#     "temp_Madrid", "temp_min_Madrid", "temp_max_Madrid", "pressure_Madrid",
#     "humidity_Madrid", "wind_speed_Madrid", "wind_deg_Madrid", "rain_1h_Madrid",
#     "snow_3h_Madrid", "clouds_all_Madrid", "temp_Seville", "temp_min_Seville",
#     "temp_max_Seville", "pressure_Seville", "humidity_Seville",
#     "wind_speed_Seville", "wind_deg_Seville", "rain_1h_Seville",
#     "snow_3h_Seville", "clouds_all_Seville", "temp_Valencia",
#     "temp_min_Valencia", "temp_max_Valencia", "pressure_Valencia",
#     "humidity_Valencia", "wind_speed_Valencia", "wind_deg_Valencia",
#     "rain_1h_Valencia", "snow_3h_Valencia", "clouds_all_Valencia"
# ]

# # Define a Pandas UDF for scaling
# @pandas_udf(joined_stream.schema, PandasUDFType.GROUPED_MAP)
# def scale_data(pdf):
#     for column in scaler_columns:
#         pdf[column] = pdf[column].apply(lambda x: scaler_X.transform([[x]])[0][0])
#     return pdf

