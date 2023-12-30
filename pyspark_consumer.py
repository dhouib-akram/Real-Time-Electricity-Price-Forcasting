
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

#from elasticsearch import Elasticsearch

spark = SparkSession.builder.appName("electricity").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the schema for weather data
weather_schema = StructType().add("dt_iso", StringType()) \
    .add("city_name", StringType()) \
    .add("temp", DoubleType()) \
    .add("temp_min", DoubleType()) \
    .add("temp_max", DoubleType()) \
    .add("pressure", IntegerType()) \
    .add("humidity", IntegerType()) \
    .add("wind_speed", DoubleType()) \
    .add("wind_deg", IntegerType()) \
    .add("rain_1h", DoubleType()) \
    .add("rain_3h", DoubleType()) \
    .add("snow_3h", DoubleType()) \
    .add("clouds_all", IntegerType()) \
    .add("weather_id", IntegerType()) \
    .add("weather_main", StringType()) \
    .add("weather_description", StringType()) \
    .add("weather_icon", StringType())

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
    .add("total load forecast", DoubleType()) \
    .add("total load actual", DoubleType()) \
    .add("price day ahead", DoubleType()) \
    .add("price actual", DoubleType())

#*************************************Energy Data Process**************************************
energy_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "energy") \
    .load()\
    .selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), energy_schema).alias("energy_data"))\
    .select("energy_data.*")


energy_df = energy_df\
     .withColumn("timestamp", col("time").cast(TimestampType())).drop("time")

energy_columns_to_drop = ['generation fossil coal-derived gas','generation fossil oil shale',
                            'generation fossil peat', 'generation geothermal',
                            'generation hydro pumped storage aggregated', 'generation marine',
                            'generation wind offshore', 'forecast wind offshore eday ahead',
                            'total load forecast', 'forecast solar day ahead',
                            'forecast wind onshore day ahead']

deduplicated_energy_stream = energy_df \
    .drop(*energy_columns_to_drop)\
    .withWatermark("timestamp", "1 hour")\
    .dropDuplicates(["timestamp"])\

   
query1 = deduplicated_energy_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 1000000)\
    .start()
data = deduplicated_energy_stream.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append")\
        .option("es.nodes", "elasticsearch")\
        .option("es.port", "9200")\
        .option("es.resource", "energy")\
        .option("es.nodes.wan.only", "true") \
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/energy") \
        .start()

#*************************************Weather Data Process**************************************
weather_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather") \
    .load()\
    .selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), weather_schema).alias("weather_data"))\
    .select("weather_data.*")

weather_columns_to_drop = ['weather_main', 'weather_id',
                              'weather_description', 'weather_icon',"rain_1h","rain_3h"]
weather_df = weather_df\
    .withColumn("timestamp", col("dt_iso").cast(TimestampType())).drop("dt_iso")

deduplicated_weather_stream = weather_df \
    .drop(*weather_columns_to_drop)\
    .withWatermark("timestamp", "1 hour")\
    .dropDuplicates(["timestamp", "city_name"])\
    .withColumn("pressure", when((col("pressure") > 1051) | (col("pressure") < 931), None).otherwise(col("pressure")))\
    .withColumn("wind_speed", when((col("wind_speed")> 50),None).otherwise(col("wind_speed")))

# df_with_window = weather_df.groupBy(
#     window(weather_df.timestamp, "3 seconds"),
#     weather_df.city_name
# ).count()


query2 = deduplicated_weather_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()



data = deduplicated_weather_stream.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append")\
        .option("es.nodes", "elasticsearch")\
        .option("es.port", "9200")\
        .option("es.resource", "weather")\
        .option("es.nodes.wan.only", "true") \
        .option("checkpointLocation", "/opt/bitnami/spark/checkpoints/weather") \
        .start()
data.awaitTermination()

# def stream_batches(dfb,batch):
#     df=  dfb.write.format("org.elasticsearch.spark.sql") \
#             .option("es.port", "9200") \
#             .option("es.nodes", "127.0.0.1") \
#             .mode("append") \
#             .option("es.nodes.wan.only", "true") \
#             .option("checkpointLocation", "/home/sou/labs/Bike_Project/checkpoints") \
#             .option("es.resource", "bike")\
#             .save()

# write_df = json_df.writeStream.outputMode("update").foreachBatch(stream_batches).start()

