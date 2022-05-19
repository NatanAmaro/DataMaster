#TODO Arquivo unutilizado estrategia alterada.
from kafka import KafkaConsumer
from constants import *
from json import dumps
import findspark

findspark.init()
from pyspark.sql import SparkSession


def run():
    spark = SparkSession.builder.getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("kafka.security.protocol", "SSL") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "SantanderTweets") \
        .option("includeHeaders", "true") \
        .option("startingOffsets", "latest") \
        .option("spark.streaming.kafka.maxRatePerPartition", "50") \
        .load()

    kafka_df.show()


if __name__ == '__main__':
    run()
