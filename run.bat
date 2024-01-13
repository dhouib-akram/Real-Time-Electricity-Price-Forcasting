@REM  Copy pyspark_consumer.py to the Spark container

docker cp ./pyspark_consumer.py real-time-electricity-price-forcasting-spark-master-1:/opt/bitnami/spark/pyspark_consumer.py
@REM  Run create_index.py

python create_index.py
@REM Submit the Spark job

docker-compose exec spark-master spark-submit --class consumer --total-executor-cores 4 --executor-memory 2g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.8.2,commons-httpclient:commons-httpclient:3.1 pyspark_consumer.py >out.txt 