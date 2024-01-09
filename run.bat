docker cp ./pyspark_consumer.py real-time-electricity-price-forcasting-spark-master-1:/opt/bitnami/spark/pyspark_consumer.py
@REM docker cp ./data/model-preprocess/pca-1.pkl real-time-electricity-price-forcasting-spark-master-1:/opt/bitnami/spark/data/pca-1.pkl
@REM docker cp ./data/model-preprocess/scaler_X.pkl real-time-electricity-price-forcasting-spark-master-1:/opt/bitnami/spark/scaler_X.pkl
@REM docker cp ./data/model-preprocess/scaler_y.pkl real-time-electricity-price-forcasting-spark-master-1:/opt/bitnami/spark/data/scaler_y.pkl
python create_index.py
docker-compose exec spark-master spark-submit --class consumer --total-executor-cores 4 --executor-memory 2g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.8.2,commons-httpclient:commons-httpclient:3.1 pyspark_consumer.py >out.txt 